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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.DataModelNeo4jVersionHandler;

/**
 * @author tgaengler
 */
public class DataModelNeo4jDeprecator extends BaseNeo4jDeprecator {

	private static final Logger LOG = LoggerFactory.getLogger(DataModelNeo4jDeprecator.class);

	protected final String prefixedDataModelUri;

	public DataModelNeo4jDeprecator(final BasicNeo4jProcessor processorArg, final boolean enableVersioningArg, final String prefixedDataModelUriArg)
			throws DMPGraphException {

		super(processorArg, enableVersioningArg);

		prefixedDataModelUri = prefixedDataModelUriArg;
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new DataModelNeo4jVersionHandler(processor, enableVersioning);
		version = versionHandler.getLatestVersion();
		previousVersion = version - 1;
	}

	@Override public void work() throws DMPGraphException {

		LOG.debug("try to deprecate all statements for data model '{}'", prefixedDataModelUri);

		processor.ensureRunningTx();

		final ResourceIterator<Node> seedNodes = processor.getDatabase()
				.findNodes(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.DATA_MODEL_PROPERTY, prefixedDataModelUri);

		if (seedNodes == null) {

			LOG.debug("there are no nodes for data model '{}' in the graph", prefixedDataModelUri);

			return;
		}

		if (!seedNodes.hasNext()) {

			LOG.debug("there are no nodes for data model '{}' in the graph", prefixedDataModelUri);
		}

		while (seedNodes.hasNext()) {

			final Node seedNode = seedNodes.next();

			startNodeHandler.handleNode(seedNode);
		}

		LOG.debug("finished deprecating all statements for data model '{}'", prefixedDataModelUri);
	}
}
