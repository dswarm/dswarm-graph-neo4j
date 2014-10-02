package org.dswarm.graph;

import java.util.Map;

import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class DataModelNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(DataModelNeo4jProcessor.class);

	private Index<Relationship>	statementUUIDsWDataModel;

	private final String				dataModelURI;

	public DataModelNeo4jProcessor(final GraphDatabaseService database, final String dataModelURIArg) throws DMPGraphException {

		super(database);

		dataModelURI = dataModelURIArg;
	}

	@Override protected void initIndices() throws DMPGraphException {

		super.initIndices();

		try {

			statementUUIDsWDataModel = database.index().forRelationships("statement_uuids_w_data_model");
		} catch (final Exception e) {

			failTx();

			final String message = "couldn't load indices successfully";

			DataModelNeo4jProcessor.LOG.error(message, e);
			DataModelNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	public Index<Relationship> getStatementWDataModelIndex() {

		return statementUUIDsWDataModel;
	}

	public String getDataModelURI() {

		return dataModelURI;
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Node node, final String URI, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + this.dataModelURI);
		} else {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + optionalDataModelURI.get());
		}
	}

	@Override
	public void handleObjectDataModel(final Node node, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
		} else {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Node node, String URI, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + this.dataModelURI);
		} else {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + optionalDataModelURI.get());
		}
	}

	@Override
	public void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDsWDataModel.add(rel, GraphStatics.UUID_W_DATA_MODEL, dataModelURI + "." + statementUUID);
	}

	@Override
	public IndexHits<Node> getResourceNodeHits(final String resourceURI) {

		return resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL, resourceURI + dataModelURI);
	}

	@Override
	public Relationship prepareRelationship(final Node subjectNode, final String predicateURI, final Node objectNode, final String statementUUID,
			final Optional<Map<String, Object>> qualifiedAttributes, final VersionHandler versionHandler) {

		final Relationship rel = super.prepareRelationship(subjectNode, predicateURI, objectNode, statementUUID, qualifiedAttributes, versionHandler);

		rel.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);

		rel.setProperty(VersioningStatics.VALID_FROM_PROPERTY, versionHandler.getRange().from());
		rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getRange().to());

		return rel;
	}
}
