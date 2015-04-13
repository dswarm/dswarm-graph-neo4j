/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.batch;

import java.util.Map;

import org.dswarm.common.types.Tuple;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class DataModelNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(DataModelNeo4jProcessor.class);

	private BatchInserterIndex			statementUUIDsWDataModel;
	private BatchInserterIndexProvider statementUUIDsWDataModelProvider;

	// TODO: utilise temp index (if necessary)
	private final ObjectLongMap<String> tempStatementUUIDsWDataModelIndex;

	private final String dataModelURI;

	public DataModelNeo4jProcessor(final BatchInserter inserter, final String dataModelURIArg) throws DMPGraphException {

		super(inserter);

		dataModelURI = dataModelURIArg;

		tempStatementUUIDsWDataModelIndex = new ObjectLongOpenHashMap<>();

		initStatementIndex();
	}

	@Override
	protected void initIndices() throws DMPGraphException {

		super.initIndices();

		// initStatementIndex();
	}

	private void initStatementIndex() throws DMPGraphException {

		try {

			final Tuple<BatchInserterIndex, BatchInserterIndexProvider> statementUUIDsWDataModelIndexTuple = getOrCreateIndex(GraphIndexStatics.STATEMENT_UUIDS_W_DATA_MODEL_INDEX_NAME, GraphStatics.UUID_W_DATA_MODEL,
					false, 1);
			statementUUIDsWDataModel = statementUUIDsWDataModelIndexTuple.v1();
			statementUUIDsWDataModelProvider = statementUUIDsWDataModelIndexTuple.v2();
		} catch (final Exception e) {

			final String message = "couldn't load indices successfully";

			DataModelNeo4jProcessor.LOG.error(message, e);
			DataModelNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	public void addToStatementWDataModelIndex(final String key, final long nodeId) {

		statementUUIDsWDataModel.add(nodeId, MapUtil.map(GraphStatics.UUID_W_DATA_MODEL, key));
	}

	public String getDataModelURI() {

		return dataModelURI;
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final long nodeId, final String URI, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			addToResourcesWDataModelIndex(URI + dataModelURI, nodeId);
		} else {

			addToResourcesWDataModelIndex(URI + optionalDataModelURI.get(), nodeId);
		}
	}

	@Override
	public void handleObjectDataModel(final Map<String, Object> objectNodeProperties, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			objectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
		} else {

			objectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Map<String, Object> subjectNodeProperties, final String URI, final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			subjectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
		} else {

			subjectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI);
		}
	}

	@Override
	public void addStatementToIndex(final long relId, final String statementUUID) {

		addToStatementWDataModelIndex(dataModelURI + "." + statementUUID, relId);
	}

	@Override
	public Optional<Long> getResourceNodeHits(final String resourceURI) {

		return getNodeIdFromResourcesWDataModelIndex(resourceURI + dataModelURI);
	}

	@Override protected String putSaltToStatementHash(final String hash) {

		return hash + " " + this.dataModelURI;
	}

	@Override
	public Map<String, Object> prepareRelationship(final String statementUUID, final Optional<Map<String, Object>> qualifiedAttributes) {

		final Map<String, Object> relProperties = super.prepareRelationship(statementUUID, qualifiedAttributes);

		relProperties.put(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);

		return relProperties;
	}

	@Override
	public void flushStatementIndices() {

		super.flushStatementIndices();

		statementUUIDsWDataModel.flush();
		statementUUIDsWDataModelProvider.shutdown();
	}

	@Override
	protected void clearTempStatementIndices() {

		super.clearTempStatementIndices();

		tempStatementUUIDsWDataModelIndex.clear();
	}
}
