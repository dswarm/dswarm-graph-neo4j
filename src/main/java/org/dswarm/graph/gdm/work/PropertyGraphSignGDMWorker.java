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

import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.read.RelationshipHandler;
import org.dswarm.graph.utils.GraphUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class PropertyGraphSignGDMWorker implements GDMWorker {

	private static final Logger			LOG							= LoggerFactory.getLogger(PropertyGraphSignGDMWorker.class);

	private final NodeHandler			nodeHandler;
	private final NodeHandler			startNodeHandler;
	private final RelationshipHandler	relationshipHandler;

	private final String				resourceUri;

	private final GraphDatabaseService	database;

	private final Map<Long, Statement>	currentResourceStatements	= new HashMap<Long, Statement>();

	public PropertyGraphSignGDMWorker(final String resourceUriArg, final GraphDatabaseService databaseArg) {

		resourceUri = resourceUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public void work() throws DMPGraphException {

		try(final Transaction tx = database.beginTx()) {

			PropertyGraphSignGDMWorker.LOG.debug("start sign GDM TX");

			final Label recordClassLabel = GraphProcessingStatics.LEAF_LABEL;

			final ResourceIterator<Node> leafNodes = database.findNodes(recordClassLabel, GraphProcessingStatics.LEAF_IDENTIFIER, true);

			if (leafNodes == null) {

				// no leaves (?)

				tx.success();

				PropertyGraphSignGDMWorker.LOG.debug("finished sign GDM TX successfully");

				return;
			}

			while(leafNodes.hasNext()) {

				final Node leafNode = leafNodes.next();

				startNodeHandler.handleNode(leafNode);
			}

			leafNodes.close();
			tx.success();

			PropertyGraphSignGDMWorker.LOG.debug("finished sign GDM TX successfully");
		} catch (final Exception e) {

			final String message = "couldn't finished sign GDM TX successfully";

			PropertyGraphSignGDMWorker.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	private class CBDNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.INCOMING);

				for (final Relationship relationship : relationships) {

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDStartNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

			for (final Relationship relationship : relationships) {

				relationshipHandler.handleRelationship(relationship);
			}
		}
	}

	private class CBDRelationshipHandler implements RelationshipHandler {

		final Map<Long, org.dswarm.graph.json.Node>	bnodes			= new HashMap<Long, org.dswarm.graph.json.Node>();
		final Map<String, ResourceNode>				resourceNodes	= new HashMap<String, ResourceNode>();

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			final long statementId = rel.getId();

			// object

			final Node objectNode = rel.getEndNode();
			final long objectId = rel.getEndNode().getId();
			final NodeType objectNodeType = GraphUtils.determineNodeType(objectNode);

			final org.dswarm.graph.json.Node objectGDMNode;

			switch (objectNodeType) {

				case Resource:
				case TypeResource:

					final String objectURI = (String) objectNode.getProperty(GraphStatics.URI_PROPERTY, null);

					if (objectURI == null) {

						final String message = "object URI can't be null";

						PropertyGraphSignGDMWorker.LOG.error(message);

						throw new DMPGraphException(message);
					}

					objectGDMNode = createResourceFromURI(objectId, objectURI);

					break;
				case BNode:
				case TypeBNode:

					objectGDMNode = createResourceFromBNode(objectId);

					break;
				case Literal:

					final Node endNode = objectNode;
					final String object = (String) endNode.getProperty(GraphStatics.VALUE_PROPERTY, null);

					if (object == null) {

						final String message = "object value can't be null";

						PropertyGraphSignGDMWorker.LOG.error(message);

						throw new DMPGraphException(message);
					}

					objectGDMNode = new LiteralNode(objectId, object);

					break;
				default:

					final String message = "unknown node type " + objectNodeType.getName() + " for object node";

					PropertyGraphSignGDMWorker.LOG.error(message);

					throw new DMPGraphException(message);
			}

			if (!objectNodeType.equals(NodeType.Literal)) {

				// continue traversal with object node
				nodeHandler.handleNode(rel.getEndNode());
			}

			// predicate

			final String predicate = rel.getType().name();
			final Predicate predicateProperty = new Predicate(predicate);

			// subject

			final Node subjectNode = rel.getStartNode();
			final NodeType subjectNodeType = GraphUtils.determineNodeType(subjectNode);

			switch (subjectNodeType) {

				case Resource:
				case TypeResource:

					break;
				case BNode:
				case TypeBNode:

					nodeHandler.handleNode(subjectNode);

					break;
			}
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
	}
}
