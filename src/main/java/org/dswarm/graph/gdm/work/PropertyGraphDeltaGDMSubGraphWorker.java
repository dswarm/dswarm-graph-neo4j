/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.gdm.work;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.utils.GraphUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * @author tgaengler
 */
public class PropertyGraphDeltaGDMSubGraphWorker implements GDMSubGraphWorker {

	private static final Logger					LOG							= LoggerFactory.getLogger(PropertyGraphDeltaGDMSubGraphWorker.class);

	private final SubGraphNodeHandler			nodeHandler;
	private final NodeHandler					startNodeHandler;
	private final SubGraphRelationshipHandler	relationshipHandler;

	private final String						resourceURI;
	private final DeltaState					deltaState;

	private final GraphDatabaseService			database;

	private final Map<String, Statement>			currentSubGraphs	= new LinkedHashMap<>();
	private final List<Path> subGraphPaths = new ArrayList<>();

	final Map<Long, org.dswarm.graph.json.Node>	bnodes			= new HashMap<Long, org.dswarm.graph.json.Node>();
	final Map<String, ResourceNode>				resourceNodes	= new HashMap<String, ResourceNode>();
	final Map<String, Predicate> predicates = new HashMap<>();

	public PropertyGraphDeltaGDMSubGraphWorker(final String resourceURIArg, final DeltaState deltaStateArg, final GraphDatabaseService databaseArg) {

		resourceURI = resourceURIArg;
		deltaState = deltaStateArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Map<String, Statement> work() throws DMPGraphException {

		try(final Transaction tx = database.beginTx()) {

			PropertyGraphDeltaGDMSubGraphWorker.LOG.debug("start delta GDM TX");

			final Node recordNode = GraphDBUtil.getResourceNode(database, resourceURI);

			if (recordNode == null) {

				PropertyGraphDeltaGDMSubGraphWorker.LOG.debug("couldn't find record for resource '" + resourceURI + "'");

				tx.success();

				PropertyGraphDeltaGDMSubGraphWorker.LOG.debug("finished delta GDM TX successfully");

				return null;
			}

			startNodeHandler.handleNode(recordNode);

			// GraphDBUtil.printPaths(subGraphPaths);

			// convert paths to statement collections
			for (final Path subGraphPath : subGraphPaths) {

				String stmtIdentifier = null;

				if(deltaState.equals(DeltaState.MODIFICATION)) {

					stmtIdentifier = Long.valueOf(subGraphPath.endNode().getId()).toString();
				}

				for (final Relationship rel : subGraphPath.relationships()) {

					if(!deltaState.equals(DeltaState.MODIFICATION)) {

						stmtIdentifier = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);
					}

					if (currentSubGraphs.containsKey(stmtIdentifier)) {

						continue;
					}

					final org.dswarm.graph.json.Node subject = getNode(rel.getStartNode());
					final Predicate predicate = getPredicate(rel.getType().name());
					final org.dswarm.graph.json.Node object = getNode(rel.getEndNode());
					final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);
					final String uuid = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);

					final Statement statement = new Statement(subject, predicate, object);

					if(order != null) {

						statement.setOrder(order);
					}

					if(uuid != null) {

						statement.setUUID(uuid);
					}

					currentSubGraphs.put(stmtIdentifier, statement);
				}
			}

			tx.success();

			PropertyGraphDeltaGDMSubGraphWorker.LOG.debug("finished delta GDM TX successfully");
		} catch (final Exception e) {

			final String message = "couldn't finished delta GDM TX successfully";

			PropertyGraphDeltaGDMSubGraphWorker.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		return currentSubGraphs;
	}

	private Predicate getPredicate(final String predicateName) {

		if(!predicates.containsKey(predicateName)) {

			predicates.put(predicateName, new Predicate(predicateName));
		}

		return predicates.get(predicateName);
	}

	private org.dswarm.graph.json.Node getNode(final Node node) throws DMPGraphException {

		final NodeType nodeType = GraphUtils.determineNodeType(node);

		final org.dswarm.graph.json.Node gdmNode;
		final long id = node.getId();

		switch(nodeType) {

			case Resource:
			case TypeResource:
				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				gdmNode = createResourceFromURI(id, uri);

				break;
			case BNode:
			case TypeBNode:

				gdmNode = createResourceFromBNode(id);

				break;
			case Literal:

				final String value = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);
				gdmNode = new LiteralNode(id, value);

				break;
			default:

				gdmNode = null;
		}

		return gdmNode;
	}

	private org.dswarm.graph.json.Node createResourceFromBNode(final long bnodeId) {

		if (!bnodes.containsKey(bnodeId)) {

			bnodes.put(bnodeId, new org.dswarm.graph.json.Node(bnodeId));
		}

		return bnodes.get(bnodeId);
	}

	private ResourceNode createResourceFromURI(final long id, final String uri) {

		if (!resourceNodes.containsKey(uri)) {

			resourceNodes.put(uri, new ResourceNode(id, uri));
		}

		return resourceNodes.get(uri);
	}

	private class CBDNodeHandler implements SubGraphNodeHandler {

		@Override
		public void handleNode(final Node node, final Path path) throws DMPGraphException {

			final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

			// copy path, if multiple relationships available for the node
			final Map<Relationship, Path> relPaths = new LinkedHashMap<>();
			int i = 0;

			for (final Relationship relationship : relationships) {

				final Path currentPath;

				if(path == null) {

					currentPath = null;
				}
				else if(i > 0) {

					try {

						currentPath = (Path) ((DeltaPath) path).clone();
					} catch (final CloneNotSupportedException e) {

						final String message = "couldn't duplicate working path";

						LOG.error(message);

						throw new DMPGraphException(message);
					}
				} else {

					currentPath = path;
				}

				relPaths.put(relationship, currentPath);

				i++;
			}

			for(final Map.Entry<Relationship, Path> relPathEntry : relPaths.entrySet()) {

				relationshipHandler.handleRelationship(relPathEntry.getKey(), relPathEntry.getValue());
			}

			if(i == 0 && path != null) {

				subGraphPaths.add(path);
			}
		}
	}

	private class CBDStartNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

			for (final Relationship relationship : relationships) {

				relationshipHandler.handleRelationship(relationship, null);
			}
		}
	}

	private class CBDRelationshipHandler implements SubGraphRelationshipHandler {

		@Override
		public void handleRelationship(final Relationship rel, final Path path) throws DMPGraphException {

			final String deltaStateString = (String) rel.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);

			if (deltaStateString == null) {

				throw new DMPGraphException("statement " + rel.getId() + " should have a delta state");
			}

			final DeltaState currentDeltaState = DeltaState.getByName(deltaStateString);

			if(!deltaState.equals(currentDeltaState)) {

				nodeHandler.handleNode(rel.getEndNode(), null);

				return;
			}

			final Path currentPath;

			if(path != null) {

				currentPath = path;
				((DeltaPath) currentPath).addRelationship(rel);
			} else {

				currentPath = new DeltaPath(rel);
			}

			nodeHandler.handleNode(rel.getEndNode(), currentPath);
		}
	}

	private class DeltaPath implements Path, Cloneable {

		private final Node startNode;
		private final Map<Long, Relationship> rels;

		public DeltaPath(final Relationship startRel) {

			rels = new LinkedHashMap<>();
			startNode = startRel.getStartNode();
			addRelationship(startRel);
		}

		public DeltaPath(final Map<Long, Relationship> relsArg) {

			startNode = Iterables.getFirst(relsArg.values(), null).getStartNode();
			rels = relsArg;
		}

		public void addRelationship(final Relationship rel) {

			rels.put(rel.getId(), rel);
		}

		@Override
		public Node startNode() {

			return startNode;
		}

		@Override
		public Node endNode() {

			return Iterables.getLast(rels.values()).getEndNode();
		}

		@Override
		public Relationship lastRelationship() {

			return Iterables.getLast(rels.values());
		}

		@Override
		public Iterable<Relationship> relationships() {

			return rels.values();
		}

		@Override
		public Iterable<Relationship> reverseRelationships() {

			return null;
		}

		@Override
		public Iterable<Node> nodes() {

			final Set<Node> nodes = new LinkedHashSet<>();

			for(final Relationship rel : rels.values()) {

				nodes.add(rel.getStartNode());
				nodes.add(rel.getEndNode());
			}

			return nodes;
		}

		@Override
		public Iterable<Node> reverseNodes() {

			return null;
		}

		@Override
		public int length() {

			return rels.size();
		}

		@Override
		public Iterator<PropertyContainer> iterator() {

			return null;
		}

		@Override protected Object clone() throws CloneNotSupportedException {

			final Map<Long, Relationship> newRels = new LinkedHashMap<>();

			for(final Map.Entry<Long, Relationship> relEntry : rels.entrySet()) {

				newRels.put(relEntry.getKey(), relEntry.getValue());
			}

			return new DeltaPath(newRels);
		}
	}
}
