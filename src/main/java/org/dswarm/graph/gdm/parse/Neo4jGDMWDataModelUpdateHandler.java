package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Neo4jGDMWDataModelUpdateHandler extends Neo4jBaseGDMUpdateHandler {

	private static final Logger			LOG	= LoggerFactory.getLogger(Neo4jGDMWDataModelUpdateHandler.class);

	private final Index<Relationship>	statementUUIDsWDataModel;

	private final String				dataModelURI;

	public Neo4jGDMWDataModelUpdateHandler(final GraphDatabaseService database, final String dataModelURIArg) throws DMPGraphException {

		super(database);

		try {

			statementUUIDsWDataModel = database.index().forRelationships("statement_uuids_w_data_model");

			dataModelURI = dataModelURIArg;

			init();
		} catch (final Exception e) {

			tx.failure();
			tx.close();

			final String message = "couldn't load indices successfully";

			Neo4jGDMWDataModelUpdateHandler.LOG.error(message, e);
			Neo4jGDMWDataModelUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	protected void setLatestVersion(final String dataModelURI) throws DMPGraphException {

		final String finalDataModelURI;

		if (dataModelURI != null) {

			finalDataModelURI = dataModelURI;
		} else {

			finalDataModelURI = this.dataModelURI;
		}

		super.setLatestVersion(finalDataModelURI);
	}

	@Override
	protected void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI) {

		if (dataModelURI == null) {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + this.dataModelURI);
		} else {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + dataModelURI);
		}
	}

	@Override
	protected void handleObjectDataModel(final Node node, final String dataModelURI) {

		if (dataModelURI == null) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
		} else {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
		}
	}

	@Override
	protected void handleSubjectDataModel(final Node node, String URI, final String dataModelURI) {

		if (dataModelURI == null) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + this.dataModelURI);
		} else {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + dataModelURI);
		}
	}

	@Override
	protected void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDsWDataModel.add(rel, GraphStatics.UUID_W_DATA_MODEL, dataModelURI + "." + statementUUID);
	}

	@Override
	protected Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index) {

		final Relationship rel = super.prepareRelationship(subjectNode, objectNode, statementUUID, statement, index);

		rel.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);

		return rel;
	}

	@Override
	protected IndexHits<Node> getResourceNodeHits(final ResourceNode resource) {

		return resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL, resource.getUri() + dataModelURI);
	}

	@Override
	protected int retrieveLatestVersion() {

		int latestVersion = 1;

		final IndexHits<Node> hits = resources.get(GraphStatics.URI, dataModelURI);

		if (hits != null && hits.hasNext()) {

			final Node dataModelNode = hits.next();
			final Integer latestVersionFromDB = (Integer) dataModelNode.getProperty(VersioningStatics.LATEST_VERSION_PROPERTY, null);

			if (latestVersionFromDB != null) {

				latestVersion = latestVersionFromDB;
			}
		}

		if (hits != null) {

			hits.close();
		}

		return latestVersion;
	}

	@Override
	protected Relationship getRelationship(final String uuid) {

		final IndexHits<Relationship> hits = statementUUIDsWDataModel.get(GraphStatics.UUID_W_DATA_MODEL, dataModelURI + "." + uuid);

		if (hits != null && hits.hasNext()) {

			final Relationship rel = hits.next();

			hits.close();

			return rel;
		}

		if (hits != null) {

			hits.close();
		}

		return null;
	}

	@Override
	public void updateLatestVersion() throws DMPGraphException {

		try {

			final IndexHits<Node> hits = resources.get(GraphStatics.URI, dataModelURI);

			if (hits != null && hits.hasNext()) {

				final Node dataModelNode = hits.next();
				dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, latestVersion);
			}

			if (hits != null) {

				hits.close();
			}
		} catch (final Exception e) {

			final String message = "couldn't update latest version";

			Neo4jGDMWDataModelUpdateHandler.LOG.error(message, e);
			Neo4jGDMWDataModelUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			tx.failure();
			tx.close();

			throw new DMPGraphException(message);
		}
	}
}
