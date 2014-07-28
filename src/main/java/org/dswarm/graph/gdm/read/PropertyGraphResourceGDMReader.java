package org.dswarm.graph.gdm.read;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.Resource;
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * retrieves the CBD for the given resource URI + provenance graph URI
 *
 * @author tgaengler
 */
public class PropertyGraphResourceGDMReader implements GDMReader {

	private static final Logger			LOG							= LoggerFactory.getLogger(PropertyGraphResourceGDMReader.class);

	private final NodeHandler         nodeHandler;
	private final NodeHandler         startNodeHandler;
	private final RelationshipHandler relationshipHandler;

	private final String recordUri;
	private final String resourceGraphUri;

	private final GraphDatabaseService database;

	private Model    model;
	private Resource currentResource;
	private final Map<Long, Statement> currentResourceStatements = new HashMap<Long, Statement>();

	public PropertyGraphResourceGDMReader(final String recordUriArg, final String resourceGraphUriArg, final GraphDatabaseService databaseArg) {

		recordUri = recordUriArg;
		resourceGraphUri = resourceGraphUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Model read() {

		final Transaction tx = database.beginTx();

		PropertyGraphResourceGDMReader.LOG.debug("start read GDM TX");

		try {

			final Index<Node> resourcesWProvenance = database.index().forNodes("resources_w_provenance");

			final IndexHits<Node> hits = resourcesWProvenance.get(GraphStatics.URI_W_PROVENANCE, recordUri + resourceGraphUri);

			if(hits == null) {

				return null;
			}

			if(!hits.hasNext()) {

				return null;
			}

			model = new Model();

			for (final Node recordNode : hits) {

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

			PropertyGraphResourceGDMReader.LOG.error("couldn't finished read GDM TX successfully", e);

			tx.failure();
			tx.close();
		} finally {

			PropertyGraphResourceGDMReader.LOG.debug("finished read GDM TX finally");

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

		final Map<Long, org.dswarm.graph.json.Node>	bnodes			= new HashMap<Long, org.dswarm.graph.json.Node>();
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

				final org.dswarm.graph.json.Node subjectGDMNode;

				switch (subjectNodeType) {

					case Resource:
					case TypeResource:

						final String subjectURI = (String) subjectNode.getProperty(GraphStatics.URI_PROPERTY, null);

						if (subjectURI == null) {

							final String message = "subject URI can't be null";

							PropertyGraphResourceGDMReader.LOG.error(message);

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

						PropertyGraphResourceGDMReader.LOG.error(message);

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

							PropertyGraphResourceGDMReader.LOG.error(message);

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

							PropertyGraphResourceGDMReader.LOG.error(message);

							throw new DMPGraphException(message);
						}

						objectGDMNode = new LiteralNode(objectId, object);

						break;
					default:

						final String message = "unknown node type " + objectNodeType.getName() + " for object node";

						PropertyGraphResourceGDMReader.LOG.error(message);

						throw new DMPGraphException(message);
				}

				// qualified properties at relationship (statement)

				final String uuid = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);
				final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

				final Statement statement;

				if (order != null && uuid != null) {

					statement = new Statement(statementId, uuid, subjectGDMNode, predicateProperty, objectGDMNode, order);
				} else if(order != null && uuid == null) {

					statement = new Statement(statementId, subjectGDMNode, predicateProperty, objectGDMNode, order);
				} else if(order == null && uuid != null) {

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

			if (!bnodes.containsKey(Long.valueOf(bnodeId))) {

				bnodes.put(Long.valueOf(bnodeId), new org.dswarm.graph.json.Node(bnodeId));
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
