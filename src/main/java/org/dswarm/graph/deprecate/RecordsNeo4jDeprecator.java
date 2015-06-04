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
package org.dswarm.graph.deprecate;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.DataModelNeo4jVersionHandler;

/**
 * @author tgaengler
 */
public class RecordsNeo4jDeprecator extends BaseNeo4jDeprecator {

	private static final Logger LOG = LoggerFactory.getLogger(RecordsNeo4jDeprecator.class);
	public static final String RECORD_VARIABLE = "record";

	private final String             prefixedDataModelUri;
	private final Collection<String> prefixedRecordURIs;

	public RecordsNeo4jDeprecator(final BasicNeo4jProcessor processorArg, final boolean enableVersioningArg, final String prefixedDataModelUriArg,
			final Collection<String> prefixedRecordURIsArg)
			throws DMPGraphException {

		super(processorArg, enableVersioningArg);

		prefixedDataModelUri = prefixedDataModelUriArg;
		prefixedRecordURIs = prefixedRecordURIsArg;
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new DataModelNeo4jVersionHandler(processor, enableVersioning);
		version = versionHandler.getLatestVersion();
		previousVersion = version - 1;
	}

	@Override public void work() throws DMPGraphException {

		LOG.debug("try to deprecate some records in data model '{}'", prefixedDataModelUri);

		processor.ensureRunningTx();

		final GraphDatabaseService database = processor.getDatabase();

		for (final String prefixedRecordURI : prefixedRecordURIs) {

			final Optional<Node> optionalRecord = getRecord(database, prefixedRecordURI);

			if(!optionalRecord.isPresent()) {

				continue;
			}

			final Node record = optionalRecord.get();

			startNodeHandler.handleNode(record);
		}

		LOG.debug("finished deprecating some records in data model '{}'", prefixedDataModelUri);
	}

	private Optional<Node> getRecord(final GraphDatabaseService database, final String prefixedRecordURI) {

		final String query = buildQuery(prefixedRecordURI);

		final Result result = database.execute(query);

		return evaluateSingleNodeResult(prefixedRecordURI, result);
	}

	private Optional<Node> evaluateSingleNodeResult(String prefixedRecordURI, Result result) {

		if(result == null) {

			LOG.error("couldn't retrieve record for uri '{}'", prefixedRecordURI);

			return Optional.empty();
		}

		if (!result.hasNext()) {

			result.close();

			LOG.error("couldn't find record for uri '{}'", prefixedRecordURI);

			return Optional.empty();
		}

		final Map<String, Object> row = result.next();

		final Iterator<Map.Entry<String, Object>> rowIter = row.entrySet().iterator();

		if (!rowIter.hasNext()) {

			result.close();

			LOG.error("couldn't find record for uri '{}'", prefixedRecordURI);

			return Optional.empty();
		}

		final Map.Entry<String, Object> column = rowIter.next();

		if (column.getValue() == null) {

			result.close();

			LOG.error("couldn't find record for uri '{}'", prefixedRecordURI);

			return Optional.empty();
		}

		if (!column.getKey().equals(RECORD_VARIABLE) || !Node.class.isInstance(column.getValue())) {

			result.close();

			return Optional.empty();
		}

		final Node record = (Node) column.getValue();

		result.close();

		return Optional.ofNullable(record);
	}

	private String buildQuery(final String prefixedRecordURI) {

		final StringBuilder sb = new StringBuilder();

		sb.append("MATCH ").append("(n:").append(NodeType.Resource).append(" {").append(GraphStatics.URI_PROPERTY).append(" : \"").append(prefixedRecordURI).append("\"})\n")
				.append("WHERE n.").append(GraphStatics.DATA_MODEL_PROPERTY).append(" = \"").append(prefixedDataModelUri).append("\"\n")
				.append("RETURN n AS ").append(RECORD_VARIABLE);

		return sb.toString();
	}
}
