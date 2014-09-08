package org.dswarm.graph.gdm.read;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.read.RelationshipHandler;
import org.dswarm.graph.utils.GraphUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * retrieves the CBD for the given resource URI + provenance graph URI
 *
 * @author tgaengler
 */
public abstract class PropertyGraphGDMResourceReader implements GDMResourceReader {

	private static final Logger			LOG							= LoggerFactory.getLogger(PropertyGraphGDMResourceReader.class);

	private final NodeHandler			nodeHandler;
	private final NodeHandler			startNodeHandler;
	private final RelationshipHandler	relationshipHandler;

	protected final String				resourceGraphUri;

	protected final GraphDatabaseService	database;

	private Resource					currentResource;
	private final Map<Long, Statement>	currentResourceStatements	= new HashMap<>();

	public PropertyGraphGDMResourceReader(final String resourceGraphUriArg, final GraphDatabaseService databaseArg) {

		resourceGraphUri = resourceGraphUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Resource read() {

		final Transaction tx = database.beginTx();

		PropertyGraphGDMResourceReader.LOG.debug("start read GDM TX");

		try {

			final Node recordNode = getResourceNode();

			if (recordNode == null) {

				LOG.debug("couldn't a find a resource node to start traversal");

				tx.success();

				return null;
			}

			final String resourceUri = (String) recordNode.getProperty(GraphStatics.URI_PROPERTY, null);

			if (resourceUri == null) {

				LOG.debug("there is no resource URI at record node '" + recordNode.getId() + "'");

				tx.success();

				return null;
			}

			currentResource = new Resource(resourceUri);
			startNodeHandler.handleNode(recordNode);

			if (!currentResourceStatements.isEmpty()) {

				// note, this is just an integer number (i.e. NOT long)
				final int mapSize = currentResourceStatements.size();

				long i = 0;

				final Set<Statement> statements = new LinkedHashSet<>();

				while (i < mapSize) {

					i++;

					final Statement statement = currentResourceStatements.get(i);

					statements.add(statement);
				}

				currentResource.setStatements(statements);
			}

			tx.success();
		} catch (final Exception e) {

			PropertyGraphGDMResourceReader.LOG.error("couldn't finished read GDM TX successfully", e);

			tx.failure();
		} finally {

			PropertyGraphGDMResourceReader.LOG.debug("finished read GDM TX finally");

			tx.close();
		}

		return currentResource;
	}

	@Override
	public long countStatements() {

		return currentResource.size();
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @return
	 */
	protected abstract Node getResourceNode();

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
			// (this is the case for model that came as GDM JSON)
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

		final Map<Long, org.dswarm.graph.json.Node>	bnodes			= new HashMap<>();
		final Map<String, ResourceNode> resourceNodes = new HashMap<>();

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			// note: we can also optionally check for the "resource property at the relationship (this property will only be
			// written right now for model that came as GDM JSON)
			if (rel.getProperty(GraphStatics.PROVENANCE_PROPERTY).equals(resourceGraphUri)) {

				final long statementId = rel.getId();

				// subject

				final Node subjectNode = rel.getStartNode();
				final long subjectId = subjectNode.getId();
				final NodeType subjectNodeType = GraphUtils.determineNodeType(subjectNode);

				final org.dswarm.graph.json.Node subjectGDMNode;

				switch (subjectNodeType) {

					case Resource:
					case TypeResource:

						final String subjectURI = (String) subjectNode.getProperty(GraphStatics.URI_PROPERTY, null);

						if (subjectURI == null) {

							final String message = "subject URI can't be null";

							PropertyGraphGDMResourceReader.LOG.error(message);

							throw new DMPGraphException(message);
						}

						subjectGDMNode = createResourceFromURI(subjectId, subjectURI);

						break;
					case BNode:
					case TypeBNode:

						subjectGDMNode = createResourceFromBNode(subjectId);

						break;
					default:

						final String message = "subject node type can only be a resource (or type resource) or bnode (or type bnode)";

						PropertyGraphGDMResourceReader.LOG.error(message);

						throw new DMPGraphException(message);
				}

				// predicate

				final String predicate = rel.getType().name();
				final Predicate predicateProperty = new Predicate(predicate);

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

							PropertyGraphGDMResourceReader.LOG.error(message);

							throw new DMPGraphException(message);
						}

						objectGDMNode = createResourceFromURI(objectId, objectURI);

						break;
					case BNode:
					case TypeBNode:

						objectGDMNode = createResourceFromBNode(objectId);

						break;
					case Literal:

						final String object = (String) objectNode.getProperty(GraphStatics.VALUE_PROPERTY, null);

						if (object == null) {

							final String message = "object value can't be null";

							PropertyGraphGDMResourceReader.LOG.error(message);

							throw new DMPGraphException(message);
						}

						objectGDMNode = new LiteralNode(objectId, object);

						break;
					default:

						final String message = "unknown node type " + objectNodeType.getName() + " for object node";

						PropertyGraphGDMResourceReader.LOG.error(message);

						throw new DMPGraphException(message);
				}

				// qualified properties at relationship (statement)

				final String uuid = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);
				final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

				final Statement statement;

				if (order != null && uuid != null) {

					statement = new Statement(statementId, uuid, subjectGDMNode, predicateProperty, objectGDMNode, order);
				} else if (order != null && uuid == null) {

					statement = new Statement(statementId, subjectGDMNode, predicateProperty, objectGDMNode, order);
				} else if (order == null && uuid != null) {

					statement = new Statement(statementId, uuid, subjectGDMNode, predicateProperty, objectGDMNode);
				} else {

					statement = new Statement(statementId, subjectGDMNode, predicateProperty, objectGDMNode);
				}

				// index should never be null (when resource was written as GDM JSON)
				final Long index = (Long) rel.getProperty(GraphStatics.INDEX_PROPERTY, null);

				if (index != null) {

					currentResourceStatements.put(index, statement);
				} else {

					// note maybe improve this here (however, this is the case for model that where written from RDF)

					currentResource.addStatement(statement);
				}

				if (!objectNodeType.equals(NodeType.Literal)) {

					// continue traversal with object node
					nodeHandler.handleNode(rel.getEndNode());
				}
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
