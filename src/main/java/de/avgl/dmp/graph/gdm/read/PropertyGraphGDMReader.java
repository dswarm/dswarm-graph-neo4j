package de.avgl.dmp.graph.gdm.read;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import de.avgl.dmp.graph.DMPGraphException;
import de.avgl.dmp.graph.GraphStatics;
import de.avgl.dmp.graph.json.LiteralNode;
import de.avgl.dmp.graph.json.Model;
import de.avgl.dmp.graph.json.Predicate;
import de.avgl.dmp.graph.json.Resource;
import de.avgl.dmp.graph.json.ResourceNode;
import de.avgl.dmp.graph.json.Statement;
import de.avgl.dmp.graph.read.NodeHandler;
import de.avgl.dmp.graph.read.RelationshipHandler;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMReader implements GDMReader {

	private static final Logger			LOG							= LoggerFactory.getLogger(PropertyGraphGDMReader.class);

	private final NodeHandler			nodeHandler;
	private final NodeHandler			startNodeHandler;
	private final RelationshipHandler	relationshipHandler;

	private final String				recordClassUri;
	private final String				resourceGraphUri;

	private final GraphDatabaseService	database;

	private Model						model;
	private Resource					currentResource;
	private Map<Long, Statement>		currentResourceStatements	= Maps.newHashMap();

	public PropertyGraphGDMReader(final String recordClassUriArg, final String resourceGraphUriArg, final GraphDatabaseService databaseArg) {

		recordClassUri = recordClassUriArg;
		resourceGraphUri = resourceGraphUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Model read() {

		final Transaction tx = database.beginTx();

		LOG.debug("start read GDM TX");

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassUri);

			final ResourceIterable<Node> recordNodes = database.findNodesByLabelAndProperty(recordClassLabel, GraphStatics.PROVENANCE_PROPERTY,
					resourceGraphUri);

			if (recordNodes == null) {

				return null;
			}

			model = new Model();

			for (final Node recordNode : recordNodes) {

				final String resourceUri = (String) recordNode.getProperty(GraphStatics.URI_PROPERTY, null);

				if (resourceUri == null) {

					// TODO: logging

					continue;
				}

				currentResource = new Resource(resourceUri);
				startNodeHandler.handleNode(recordNode);

				if (currentResourceStatements != null && !currentResourceStatements.isEmpty()) {

					// note, this is just an integer number (i.e. NOT long)
					final int mapSize = currentResourceStatements.size();

					long i = 0;

					final Set<Statement> statements = Sets.newLinkedHashSet();

					while (i < mapSize) {

						i++;

						final Statement statement = currentResourceStatements.get(Long.valueOf(i));

						statements.add(statement);
					}

					currentResource.setStatements(statements);
				}
				
				model.addResource(currentResource);
			}
		} catch (final Exception e) {

			LOG.error("couldn't finished read GDM TX successfully", e);

			tx.failure();
			tx.close();
		} finally {

			LOG.debug("finished read GDM TX finally");

			tx.success();
			tx.close();
		}

		return model;
	}

	@Override
	public long countStatements() {

		return model.size();
	}

	private class CBDNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			// => maybe we should find an appropriated cypher query as replacement for this processing
			if (!node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDStartNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDRelationshipHandler implements RelationshipHandler {

		final Map<Long, de.avgl.dmp.graph.json.Node>	bnodes			= new HashMap<Long, de.avgl.dmp.graph.json.Node>();
		final Map<String, ResourceNode>					resourceNodes	= new HashMap<String, ResourceNode>();

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			if (rel.getProperty(GraphStatics.PROVENANCE_PROPERTY).equals(resourceGraphUri)) {

				final long statementId = rel.getId();

				// TODO: utilise __NODETYPE__ property for switch

				final String subject = (String) rel.getStartNode().getProperty(GraphStatics.URI_PROPERTY, null);
				final long subjectId = rel.getStartNode().getId();

				final de.avgl.dmp.graph.json.Node subjectNode;

				if (subject == null) {

					// subject is a bnode

					subjectNode = createResourceFromBNode(subjectId);
				} else {

					subjectNode = createResourceFromURI(subjectId, subject);
				}

				final String predicate = (String) rel.getProperty(GraphStatics.URI_PROPERTY, null);
				final Predicate predicateProperty = new Predicate(predicate);

				final String object;

				final String objectURI = (String) rel.getEndNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final de.avgl.dmp.graph.json.Node objectNode;

				// TODO: utilise __NODETYPE__ property for switch

				final long objectId = rel.getEndNode().getId();

				if (objectURI != null) {

					// object is a resource

					object = objectURI;
					objectNode = createResourceFromURI(objectId, object);
				} else {

					// check, whether object is a bnode

					if (!rel.getEndNode().hasProperty(GraphStatics.VALUE_PROPERTY)) {

						// object is a bnode

						objectNode = createResourceFromBNode(objectId);

					} else {

						// object is a literal node

						object = (String) rel.getEndNode().getProperty(GraphStatics.VALUE_PROPERTY, null);

						objectNode = new LiteralNode(objectId, object);

						currentResource.addStatement(statementId, subjectNode, predicateProperty, objectNode);

						return;
					}
				}

				final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

				final Statement statement;

				if (order != null) {

					statement = new Statement(statementId, subjectNode, predicateProperty, objectNode, order);
				} else {

					statement = new Statement(statementId, subjectNode, predicateProperty, objectNode);
				}

				// index should never be null (when resource was written as GDM JSON)
				final Long index = (Long) rel.getProperty(GraphStatics.INDEX_PROPERTY, null);

				if (index != null) {

					currentResourceStatements.put(index, statement);
				} else {

					// note maybe improve this here

					currentResource.addStatement(statement);
				}

				// continue traversal with object node
				nodeHandler.handleNode(rel.getEndNode());
			}
		}

		private de.avgl.dmp.graph.json.Node createResourceFromBNode(final long bnodeId) {

			if (!bnodes.containsKey(Long.valueOf(bnodeId))) {

				bnodes.put(Long.valueOf(bnodeId), new de.avgl.dmp.graph.json.Node(bnodeId));
			}

			return bnodes.get(Long.valueOf(bnodeId));
		}

		private ResourceNode createResourceFromURI(final long id, final String uri) {

			if (!resourceNodes.containsKey(uri)) {

				resourceNodes.put(uri, new ResourceNode(id, uri));
			}

			return resourceNodes.get(uri);
		}
	}
}
