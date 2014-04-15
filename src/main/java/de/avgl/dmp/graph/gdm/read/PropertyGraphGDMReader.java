package de.avgl.dmp.graph.gdm.read;

import java.util.HashMap;
import java.util.LinkedHashSet;
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

import de.avgl.dmp.graph.DMPGraphException;
import de.avgl.dmp.graph.GraphStatics;
import de.avgl.dmp.graph.NodeType;
import de.avgl.dmp.graph.json.LiteralNode;
import de.avgl.dmp.graph.json.Model;
import de.avgl.dmp.graph.json.Predicate;
import de.avgl.dmp.graph.json.Resource;
import de.avgl.dmp.graph.json.ResourceNode;
import de.avgl.dmp.graph.json.Statement;
import de.avgl.dmp.graph.read.NodeHandler;
import de.avgl.dmp.graph.read.RelationshipHandler;
import de.avgl.dmp.graph.utils.GraphUtils;

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
	private final Map<Long, Statement>	currentResourceStatements	= new HashMap<Long, Statement>();

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

		PropertyGraphGDMReader.LOG.debug("start read GDM TX");

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

					LOG.debug("there is no resource URI at record node '" + recordNode.getId() + "'");

					continue;
				}

				currentResource = new Resource(resourceUri);
				startNodeHandler.handleNode(recordNode);

				if (currentResourceStatements != null && !currentResourceStatements.isEmpty()) {

					// note, this is just an integer number (i.e. NOT long)
					final int mapSize = currentResourceStatements.size();

					long i = 0;

					final Set<Statement> statements = new LinkedHashSet<Statement>();

					while (i < mapSize) {

						i++;

						final Statement statement = currentResourceStatements.get(Long.valueOf(i));

						statements.add(statement);
					}

					currentResource.setStatements(statements);
				}

				model.addResource(currentResource);

				currentResourceStatements.clear();
			}
		} catch (final Exception e) {

			PropertyGraphGDMReader.LOG.error("couldn't finished read GDM TX successfully", e);

			tx.failure();
			tx.close();
		} finally {

			PropertyGraphGDMReader.LOG.debug("finished read GDM TX finally");

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

		final Map<Long, de.avgl.dmp.graph.json.Node>	bnodes			= new HashMap<Long, de.avgl.dmp.graph.json.Node>();
		final Map<String, ResourceNode>					resourceNodes	= new HashMap<String, ResourceNode>();

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

				final de.avgl.dmp.graph.json.Node subjectGDMNode;

				switch (subjectNodeType) {

					case Resource:
					case TypeResource:

						final String subjectURI = (String) subjectNode.getProperty(GraphStatics.URI_PROPERTY, null);

						if (subjectURI == null) {

							final String message = "subject URI can't be null";

							PropertyGraphGDMReader.LOG.error(message);

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

						PropertyGraphGDMReader.LOG.error(message);

						throw new DMPGraphException(message);
				}

				// predicate

				final String predicate = rel.getType().name();
				final Predicate predicateProperty = new Predicate(predicate);

				// object

				final Node objectNode = rel.getEndNode();
				final long objectId = rel.getEndNode().getId();
				final NodeType objectNodeType = GraphUtils.determineNodeType(objectNode);

				final de.avgl.dmp.graph.json.Node objectGDMNode;

				switch (objectNodeType) {

					case Resource:
					case TypeResource:

						final String objectURI = (String) objectNode.getProperty(GraphStatics.URI_PROPERTY, null);

						if (objectURI == null) {

							final String message = "object URI can't be null";

							PropertyGraphGDMReader.LOG.error(message);

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

							PropertyGraphGDMReader.LOG.error(message);

							throw new DMPGraphException(message);
						}

						objectGDMNode = new LiteralNode(objectId, object);

						break;
					default:

						final String message = "unknown node type " + objectNodeType.getName() + " for object node";

						PropertyGraphGDMReader.LOG.error(message);

						throw new DMPGraphException(message);
				}

				// qualified properties at relationship (statement)

				final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

				final Statement statement;

				if (order != null) {

					statement = new Statement(statementId, subjectGDMNode, predicateProperty, objectGDMNode, order);
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
