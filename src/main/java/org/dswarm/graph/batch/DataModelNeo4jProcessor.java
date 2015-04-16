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

import com.google.common.base.Optional;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public class DataModelNeo4jProcessor extends Neo4jProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(DataModelNeo4jProcessor.class);

	private final String dataModelURI;

	public DataModelNeo4jProcessor(final BatchInserter inserter, final String dataModelURIArg) throws DMPGraphException {

		super(inserter);

		dataModelURI = dataModelURIArg;
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
	public void handleSubjectDataModel(final Map<String, Object> subjectNodeProperties, final String URI,
			final Optional<String> optionalDataModelURI) {

		if (!optionalDataModelURI.isPresent()) {

			subjectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
		} else {

			subjectNodeProperties.put(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI);
		}
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
}
