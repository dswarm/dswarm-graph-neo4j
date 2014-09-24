package org.dswarm.graph.batch;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.GraphStatics;

import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class SimpleNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(SimpleNeo4jProcessor.class);

	protected BatchInserterIndex statementUUIDs;

	private final ObjectLongMap<String> tempStatementUUIDsIndex;

	public SimpleNeo4jProcessor(final BatchInserter inserter) throws DMPGraphException {

		super(inserter);

		tempStatementUUIDsIndex = new ObjectLongOpenHashMap<>();
	}

	@Override protected void initIndices() throws DMPGraphException {

		super.initIndices();

		try {

			statementUUIDs = getOrCreateIndex("statement_uuids", GraphStatics.UUID, false);
		} catch (final Exception e) {

			final String message = "couldn't load indices successfully";

			SimpleNeo4jProcessor.LOG.error(message, e);
			SimpleNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Long nodeId, final String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			addToResourcesWDataModelIndex(URI + optionalDataModelURI.get(), nodeId);
		}
	}

	@Override
	public void handleObjectDataModel(Long nodeId, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			inserter.setNodeProperty(nodeId, GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Long nodeId, String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			inserter.setNodeProperty(nodeId, GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
			addToResourcesWDataModelIndex(URI + optionalDataModelURI.get(), nodeId);
		}
	}

	@Override
	public void addStatementToIndex(final Long relId, final String statementUUID) {

		statementUUIDs.add(relId, MapUtil.map(GraphStatics.UUID, statementUUID));
	}

	@Override
	public Optional<Long> getResourceNodeHits(final String resourceURI) {

		return getNodeIdFromResourcesIndex(resourceURI);
	}

	@Override public void flushStatementIndices() {
		
		super.flushStatementIndices();

		statementUUIDs.flush();
	}

	@Override protected void clearTempStatementIndices() {

		super.clearTempStatementIndices();

		tempStatementUUIDsIndex.clear();
	}
}
