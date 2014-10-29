package org.dswarm.graph.batch;

import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class SimpleNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(SimpleNeo4jProcessor.class);

	protected BatchInserterIndex		statementUUIDs;

	// TODO: utilise temp index (if necessary)
	private final ObjectLongMap<String>	tempStatementUUIDsIndex;

	public SimpleNeo4jProcessor(final BatchInserter inserter) throws DMPGraphException {

		super(inserter);

		tempStatementUUIDsIndex = new ObjectLongOpenHashMap<>();

		initStatementIndex();
	}

	@Override
	protected void initIndices() throws DMPGraphException {

		super.initIndices();

		// initStatementIndex();
	}

	private void initStatementIndex() throws DMPGraphException {

		try {

			statementUUIDs = getOrCreateIndex(GraphIndexStatics.STATEMENT_UUIDS_INDEX_NAME, GraphStatics.UUID, false, 1);
		} catch (final Exception e) {

			final String message = "couldn't load indices successfully";

			SimpleNeo4jProcessor.LOG.error(message, e);
			SimpleNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final long nodeId, final String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			addToResourcesWDataModelIndex(URI + optionalDataModelURI.get(), nodeId);
		}
	}

	@Override
	public void handleObjectDataModel(final Map<String, Object> objectNodeProperties, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			objectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Map<String, Object> subjectNodeProperties, final String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			subjectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void addStatementToIndex(final long relId, final String statementUUID) {

		statementUUIDs.add(relId, MapUtil.map(GraphStatics.UUID, statementUUID));
	}

	@Override
	public Optional<Long> getResourceNodeHits(final String resourceURI) {

		return getNodeIdFromResourcesIndex(resourceURI);
	}

	@Override
	public void flushStatementIndices() {

		super.flushStatementIndices();

		statementUUIDs.flush();
	}

	@Override
	protected void clearTempStatementIndices() {

		super.clearTempStatementIndices();

		tempStatementUUIDsIndex.clear();
	}
}
