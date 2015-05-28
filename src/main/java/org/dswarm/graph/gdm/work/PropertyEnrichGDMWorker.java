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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.utils.GraphUtils;

/**
 * @author tgaengler
 */
public class PropertyEnrichGDMWorker implements GDMWorker {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyEnrichGDMWorker.class);

	private final HierarchyLevelNodeHandler         nodeHandler;
	private final NodeHandler                       startNodeHandler;
	private final HierarchyLevelRelationshipHandler relationshipHandler;

	private final String prefixedResourceUri;
	private final long   resourceHash;

	private final GraphDatabaseService database;

	public PropertyEnrichGDMWorker(final String prefixedResourceUriArg, final long resourceHashArg, final GraphDatabaseService databaseArg) {

		prefixedResourceUri = prefixedResourceUriArg;
		resourceHash = resourceHashArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public void work() throws DMPGraphException {

		try (final Transaction tx = database.beginTx()) {

			PropertyEnrichGDMWorker.LOG.debug("start enrich GDM TX");

			final Node recordNode = GraphDBUtil.getResourceNode(database, prefixedResourceUri);

			if (recordNode == null) {

				PropertyEnrichGDMWorker.LOG.debug("couldn't find record for resource '{}'", prefixedResourceUri);

				tx.success();

				PropertyEnrichGDMWorker.LOG.debug("finished enrich GDM TX successfully");

				return;
			}

			startNodeHandler.handleNode(recordNode);

			tx.success();

			PropertyEnrichGDMWorker.LOG.debug("finished enrich GDM TX successfully");
		} catch (final Exception e) {

			final String message = "couldn't finished enrich GDM TX successfully";

			PropertyEnrichGDMWorker.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	private class CBDNodeHandler implements HierarchyLevelNodeHandler {

		@Override
		public void handleNode(final Node node, final int hierarchyLevel) throws DMPGraphException {

			if (node.hasProperty(GraphStatics.RESOURCE_PROPERTY) && node.getProperty(GraphStatics.RESOURCE_PROPERTY).equals(resourceHash)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					relationshipHandler.handleRelationship(relationship, hierarchyLevel);
				}
			}
		}
	}

	private class CBDStartNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					relationshipHandler.handleRelationship(relationship, 0);
				}
			}
		}
	}

	private class CBDRelationshipHandler implements HierarchyLevelRelationshipHandler {

		@Override
		public void handleRelationship(final Relationship rel, final int hierarchyLevel) throws DMPGraphException {

			// subject

			final Node subjectNode = rel.getStartNode();
			subjectNode.setProperty(DeltaStatics.HIERARCHY_LEVEL_PROPERTY, hierarchyLevel);

			// predicate

			rel.setProperty(DeltaStatics.HIERARCHY_LEVEL_PROPERTY, hierarchyLevel);

			// object

			final Node objectNode = rel.getEndNode();
			final NodeType objectNodeType = GraphUtils.determineNodeType(objectNode);

			switch (objectNodeType) {

				case Literal:
				case Resource:
				case TypeResource:

					objectNode.setProperty(DeltaStatics.HIERARCHY_LEVEL_PROPERTY, hierarchyLevel + 1);

					break;
				default:

					// note: we need to filter out bnodes without further statements, e.g., mabxml:tfType nodes have no statements (except of the optional rdf:type statement)
					if (objectNode.hasRelationship(Direction.OUTGOING)) {

						// continue traversal with object node
						nodeHandler.handleNode(objectNode, hierarchyLevel + 1);
					} else {

						// i.e. we need to stop traversal here, and set the hierarchy level property

						objectNode.setProperty(DeltaStatics.HIERARCHY_LEVEL_PROPERTY, hierarchyLevel + 1);
					}

					break;
			}
		}
	}
}
