package org.dswarm.graph.gdm.read;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.read.RelationshipHandler;
import org.dswarm.graph.versioning.Range;
import org.dswarm.graph.versioning.VersioningStatics;
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
 * @author tgaengler
 */
public class PropertyGraphGDMModelReader implements GDMModelReader {

	private static final Logger			LOG							= LoggerFactory.getLogger(PropertyGraphGDMModelReader.class);

	private final NodeHandler			nodeHandler;
	private final NodeHandler			startNodeHandler;
	private final RelationshipHandler	relationshipHandler;

	private final String				recordClassUri;
	private final String				resourceGraphUri;

	private final GraphDatabaseService	database;

	private Model						model;
	private Resource					currentResource;
	private final Map<Long, Statement>	currentResourceStatements	= new HashMap<Long, Statement>();

	private Integer					version;

	private Transaction					tx							= null;

	public PropertyGraphGDMModelReader(final String recordClassUriArg, final String resourceGraphUriArg, final Integer versionArg,
			final GraphDatabaseService databaseArg) throws DMPGraphException {

		recordClassUri = recordClassUriArg;
		resourceGraphUri = resourceGraphUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();

		if (versionArg != null) {

			version = versionArg;
		} else {

			tx = database.beginTx();

			try {

				version = getLatestVersion();
			} catch (final Exception e) {

				final String message = "couldn't retrieve latest version successfully";

				PropertyGraphGDMModelReader.LOG.error(message, e);

				tx.failure();
				tx.close();

				throw new DMPGraphException(message);
			}
		}
	}

	@Override
	public Model read() throws DMPGraphException {

		if (tx == null) {

			try {

				tx = database.beginTx();
			} catch (final Exception e) {

				final String message = "couldn't acquire tx successfully";

				PropertyGraphGDMModelReader.LOG.debug(message, e);

				throw new DMPGraphException(message);
			}
		}

		PropertyGraphGDMModelReader.LOG.debug("start read GDM TX");

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassUri);

			final ResourceIterable<Node> recordNodes = database.findNodesByLabelAndProperty(recordClassLabel, GraphStatics.PROVENANCE_PROPERTY,
					resourceGraphUri);

			if (recordNodes == null) {

				tx.success();

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

				if (!currentResourceStatements.isEmpty()) {

					// note, this is just an integer number (i.e. NOT long)
					final int mapSize = currentResourceStatements.size();

					long i = 0;

					final Set<Statement> statements = new LinkedHashSet<Statement>();

					while (i < mapSize) {

						i++;

						final Statement statement = currentResourceStatements.get(i);

						statements.add(statement);
					}

					currentResource.setStatements(statements);
				}

				model.addResource(currentResource);

				currentResourceStatements.clear();
			}

			tx.success();
		} catch (final Exception e) {

			PropertyGraphGDMModelReader.LOG.error("couldn't finished read GDM TX successfully", e);

			tx.failure();
		} finally {

			PropertyGraphGDMModelReader.LOG.debug("finished read GDM TX finally");

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

					final Integer validFrom = (Integer) relationship.getProperty(VersioningStatics.VALID_FROM_PROPERTY, null);
					final Integer validTo = (Integer) relationship.getProperty(VersioningStatics.VALID_TO_PROPERTY, null);

					if (validFrom != null && validTo != null) {

						if (Range.range(validFrom, validTo).contains(version)) {

							relationshipHandler.handleRelationship(relationship);
						}
					} else {

						// TODO: remove this later, when every stmt is versioned
						relationshipHandler.handleRelationship(relationship);
					}
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

					final Integer validFrom = (Integer) relationship.getProperty(VersioningStatics.VALID_FROM_PROPERTY, null);
					final Integer validTo = (Integer) relationship.getProperty(VersioningStatics.VALID_TO_PROPERTY, null);

					if (validFrom != null && validTo != null) {

						if (Range.range(validFrom, validTo).contains(version)) {

							relationshipHandler.handleRelationship(relationship);
						}
					} else {

						// TODO: remove this later, when every stmt is versioned
						relationshipHandler.handleRelationship(relationship);
					}
				}
			}
		}
	}

	private class CBDRelationshipHandler implements RelationshipHandler {

		private final PropertyGraphGDMReader	propertyGraphGDMReader	= new PropertyGraphGDMReader();

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			// note: we can also optionally check for the "resource property at the relationship (this property will only be
			// written right now for model that came as GDM JSON)
			if (rel.getProperty(GraphStatics.PROVENANCE_PROPERTY).equals(resourceGraphUri)) {

				final long statementId = rel.getId();

				// subject

				final Node subjectNode = rel.getStartNode();
				final org.dswarm.graph.json.Node subjectGDMNode = propertyGraphGDMReader.readSubject(subjectNode);

				// predicate

				final String predicate = rel.getType().name();
				final Predicate predicateProperty = new Predicate(predicate);

				// object

				final Node objectNode = rel.getEndNode();
				final org.dswarm.graph.json.Node objectGDMNode = propertyGraphGDMReader.readObject(objectNode);

				// qualified properties at relationship (statement)

				final String uuid = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);
				final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

				final Statement statement;

				if (order != null && uuid != null) {

					statement = new Statement(statementId, uuid, subjectGDMNode, predicateProperty, objectGDMNode, order);
				} else if (order != null) {

					statement = new Statement(statementId, subjectGDMNode, predicateProperty, objectGDMNode, order);
				} else if (uuid != null) {

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

				if (!objectGDMNode.getType().equals(org.dswarm.graph.json.NodeType.Literal)) {

					// continue traversal with object node
					nodeHandler.handleNode(rel.getEndNode());
				}
			}
		}
	}

	private int getLatestVersion() {

		int latestVersion = 1;

		final Index<Node> resources = database.index().forNodes("resources");
		final IndexHits<Node> hits = resources.get(GraphStatics.URI, resourceGraphUri);

		if (hits != null && hits.iterator().hasNext()) {

			final Node dataModelNode = hits.iterator().next();
			final Integer latestVersionFromDB = (Integer) dataModelNode.getProperty(VersioningStatics.LATEST_VERSION_PROPERTY, null);

			if (latestVersionFromDB != null) {

				latestVersion = latestVersionFromDB;
			}
		}

		if(hits != null) {

			hits.close();
		}

		return latestVersion;
	}
}
