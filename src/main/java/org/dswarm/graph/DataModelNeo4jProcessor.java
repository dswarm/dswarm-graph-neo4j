package org.dswarm.graph;

import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(DataModelNeo4jProcessor.class);

	private final Index<Relationship> statementUUIDsWDataModel;

	private final String dataModelURI;

	public DataModelNeo4jProcessor(final GraphDatabaseService database, final String dataModelURIArg) throws DMPGraphException {

		super(database);

		try {

			statementUUIDsWDataModel = database.index().forRelationships("statement_uuids_w_data_model");
		} catch (final Exception e) {

			failTx();

			final String message = "couldn't load indices successfully";

			DataModelNeo4jProcessor.LOG.error(message, e);
			DataModelNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

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
}
