package org.dswarm.graph.batch;

import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.GraphStatics;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongMap;
import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class DataModelNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(DataModelNeo4jProcessor.class);

	private BatchInserterIndex statementUUIDsWDataModel;

	private final ObjectLongMap<String> tempStatementUUIDsWDataModelIndex;

	private final String dataModelURI;

	public DataModelNeo4jProcessor(final BatchInserter inserter, final String dataModelURIArg) throws DMPGraphException {

		super(inserter);

		dataModelURI = dataModelURIArg;

		tempStatementUUIDsWDataModelIndex = new ObjectLongOpenHashMap<>();
	}

	@Override protected void initIndices() throws DMPGraphException {

		super.initIndices();

		try {

			statementUUIDsWDataModel = getOrCreateIndex("statement_uuids_w_data_model", GraphStatics.UUID_W_DATA_MODEL, false);
		} catch (final Exception e) {

			final String message = "couldn't load indices successfully";

			DataModelNeo4jProcessor.LOG.error(message, e);
			DataModelNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

//	public BatchInserterIndex getStatementWDataModelIndex() {
//
//		return statementUUIDsWDataModel;
//	}

	public void addToStatementWDataModelIndex(final String key, final Long nodeId) {

		statementUUIDsWDataModel.add(nodeId, MapUtil.map(GraphStatics.UUID_W_DATA_MODEL, key));
	}

	public String getDataModelURI() {

		return dataModelURI;
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Long nodeId, final String URI, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			addToResourcesWDataModelIndex(URI + this.dataModelURI, nodeId);
		} else {

			addToResourcesWDataModelIndex(URI + optionalDataModelURI.get(), nodeId);
		}
	}

	@Override
	public void handleObjectDataModel(final Long nodeId, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			inserter.setNodeProperty(nodeId, GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
		} else {

			inserter.setNodeProperty(nodeId, GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Long nodeId, String URI, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			inserter.setNodeProperty(nodeId, GraphStatics.DATA_MODEL_PROPERTY, this.dataModelURI);
			addToResourcesWDataModelIndex(URI + this.dataModelURI, nodeId);
		} else {

			inserter.setNodeProperty(nodeId, GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI);
			addToResourcesWDataModelIndex(URI + optionalDataModelURI.get(), nodeId);
		}
	}

	@Override
	public void addStatementToIndex(final Long relId, final String statementUUID) {

		addToStatementWDataModelIndex(dataModelURI + "." + statementUUID, relId);
	}

	@Override
	public Optional<Long> getResourceNodeHits(final String resourceURI) {

		return getNodeIdFromResourcesWDataModelIndex(resourceURI + dataModelURI);
	}

	@Override
	public Long prepareRelationship(final Long subjectNodeId, final String predicateURI, final Long objectNodeId, final String statementUUID,
			final Optional<Map<String, Object>> qualifiedAttributes) {

		final Long relId = super.prepareRelationship(subjectNodeId, predicateURI, objectNodeId, statementUUID, qualifiedAttributes);

		inserter.setRelationshipProperty(relId, GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);

		return relId;
	}

	@Override public void flushStatementIndices() {

		super.flushStatementIndices();

		statementUUIDsWDataModel.flush();
	}

	@Override protected void clearTempStatementIndices() {

		super.clearTempStatementIndices();

		tempStatementUUIDsWDataModelIndex.clear();
	}
}
