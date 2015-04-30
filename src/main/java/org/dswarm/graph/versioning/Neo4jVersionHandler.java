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

import com.google.common.base.Optional;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public abstract class Neo4jVersionHandler implements VersionHandler {

	private static final Logger LOG = LoggerFactory.getLogger(Neo4jVersionHandler.class);

	protected boolean latestVersionInitialized = false;

	protected int latestVersion;

	private Range range;

	protected final BasicNeo4jProcessor processor;

	public Neo4jVersionHandler(final BasicNeo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	@Override
	public int getLatestVersion() {

		return latestVersion;
	}

	@Override public Range getRange() {

		return range;
	}

	protected void init() {

		latestVersion = retrieveLatestVersion() + 1;
		range = Range.range(latestVersion);
	}

	protected abstract int retrieveLatestVersion();

	public void setLatestVersion(final Optional<String> optionalDataModelURI) throws DMPGraphException {

		if (!latestVersionInitialized) {

			if (!optionalDataModelURI.isPresent()) {

				return;
			}

			final String dataModelURI = optionalDataModelURI.get();
			final long resourceUriDataModelUriHash = processor
					.generateResourceHash(dataModelURI, Optional.of(VersioningStatics.VERSIONING_DATA_MODEL_URI));

			Optional<Node> optionalDataModelNode = processor.determineNode(Optional.of(NodeType.Resource), Optional.<String>absent(),
					optionalDataModelURI, Optional.of(VersioningStatics.VERSIONING_DATA_MODEL_URI), Optional.of(resourceUriDataModelUriHash));

			if (optionalDataModelNode.isPresent()) {

				latestVersionInitialized = true;

				return;
			}

			final Label dataModelLabel = processor.getLabel(VersioningStatics.DATA_MODEL_TYPE);
			final Label dataModelResourceLabel = processor.getLabel(NodeType.Resource.toString());

			final Node dataModelNode = processor.getDatabase().createNode(dataModelLabel, dataModelResourceLabel);
			dataModelNode.setProperty(GraphStatics.URI_PROPERTY, dataModelURI);
			dataModelNode.setProperty(GraphStatics.HASH, resourceUriDataModelUriHash);
			dataModelNode.setProperty(GraphStatics.DATA_MODEL_PROPERTY, VersioningStatics.VERSIONING_DATA_MODEL_URI);
			dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, range.from());

			processor.addNodeToResourcesWDataModelIndex(dataModelURI, resourceUriDataModelUriHash, dataModelNode);

			latestVersionInitialized = true;
		}
	}

}
