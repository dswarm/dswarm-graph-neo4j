package org.dswarm.graph.gdm;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
public class Neo4jGDMWDataModelProcessor extends BaseNeo4jGDMProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(Neo4jGDMWDataModelProcessor.class);

	private final Index<Relationship> statementUUIDsWDataModel;

	private final String dataModelURI;

	public Neo4jGDMWDataModelProcessor(final GraphDatabaseService database, final String dataModelURIArg) throws DMPGraphException {

		super(database);

		try {

			statementUUIDsWDataModel = database.index().forRelationships("statement_uuids_w_data_model");
		} catch (final Exception e) {

			tx.failure();
			tx.close();
			txIsClosed = true;

			final String message = "couldn't load indices successfully";

			Neo4jGDMWDataModelProcessor.LOG.error(message, e);
			Neo4jGDMWDataModelProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}

		dataModelURI = dataModelURIArg;
	}

	public Index<Relationship> getStatementWDataModelIndex() {

		return statementUUIDsWDataModel;
	}

	public String getDataModelURI() {

		return dataModelURI;
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI) {

		if (dataModelURI == null) {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + this.dataModelURI);
		} else {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + dataModelURI);
		}
	}

	@Override
	public void handleObjectDataModel(final Node node, final String dataModelURI) {

		if (dataModelURI == null) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
		} else {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
		}
	}

	@Override
	public void handleSubjectDataModel(final Node node, String URI, final String dataModelURI) {

		if (dataModelURI == null) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + this.dataModelURI);
		} else {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + dataModelURI);
		}
	}

	@Override
	public void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDsWDataModel.add(rel, GraphStatics.UUID_W_DATA_MODEL, dataModelURI + "." + statementUUID);
	}

	@Override
	public Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index, final VersionHandler versionHandler) {

		final Relationship rel = super.prepareRelationship(subjectNode, objectNode, statementUUID, statement, index, versionHandler);

		rel.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);

		rel.setProperty(VersioningStatics.VALID_FROM_PROPERTY, versionHandler.getRange().from());
		rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getRange().to());

		return rel;
	}

	@Override
	protected IndexHits<Node> getResourceNodeHits(final ResourceNode resource) {

		return resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL, resource.getUri() + dataModelURI);
	}
}
