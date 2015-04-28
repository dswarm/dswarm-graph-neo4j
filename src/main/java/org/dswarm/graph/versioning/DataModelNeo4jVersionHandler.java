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
package org.dswarm.graph.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.dswarm.graph.Neo4jProcessor;

import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class DataModelNeo4jVersionHandler extends Neo4jVersionHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelNeo4jVersionHandler.class);

	public DataModelNeo4jVersionHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);

		processor.ensureRunningTx();

		try {

			init();
		} catch (final Exception e) {

			processor.failTx();

			final String message = "couldn't init version handler successfully";

			DataModelNeo4jVersionHandler.LOG.error(message, e);
			DataModelNeo4jVersionHandler.LOG.debug("couldn't finish TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void setLatestVersion(final Optional<String> optionalDataModelURI) throws DMPGraphException {

		final String finalDataModelURI;

		if (optionalDataModelURI.isPresent()) {

			finalDataModelURI = optionalDataModelURI.get();
		} else {

			finalDataModelURI = ((DataModelNeo4jProcessor) processor).getDataModelURI();
		}

		super.setLatestVersion(Optional.fromNullable(finalDataModelURI));
	}

	@Override
	protected int retrieveLatestVersion() {

		int latestVersion = 0;

		final Optional<Node> optionalNode = processor.getNodeFromResourcesWDataModelIndex(((DataModelNeo4jProcessor) processor).getDataModelURI(), VersioningStatics.VERSIONING_DATA_MODEL_URI);

		if (optionalNode.isPresent()) {

			final Node dataModelNode = optionalNode.get();
			final Integer latestVersionFromDB = (Integer) dataModelNode.getProperty(VersioningStatics.LATEST_VERSION_PROPERTY, null);

			if (latestVersionFromDB != null) {

				latestVersion = latestVersionFromDB;
			}
		}

		return latestVersion;
	}

	@Override
	public void updateLatestVersion() throws DMPGraphException {

		processor.ensureRunningTx();

		try {

			final String dataModelURI = ((DataModelNeo4jProcessor) processor).getDataModelURI();
			final Optional<Node> optionalNode = processor.getNodeFromResourcesWDataModelIndex(dataModelURI, VersioningStatics.VERSIONING_DATA_MODEL_URI);

			if (optionalNode.isPresent()) {

				final Node dataModelNode = optionalNode.get();
				dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, latestVersion);
			} else {

				setLatestVersion(Optional.of(dataModelURI));
			}
		} catch (final Exception e) {

			processor.failTx();

			final String message = "couldn't update latest version";

			DataModelNeo4jVersionHandler.LOG.error(message, e);
			DataModelNeo4jVersionHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}
}
