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
package org.dswarm.graph.gdm.read;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMResourceByURIReader extends PropertyGraphGDMResourceReader {

	private static final String TYPE = "GDM record by URI";

	private final String recordUri;

	public PropertyGraphGDMResourceByURIReader(final String recordUriArg, final String dataModelUri, final Optional<Integer> optionalVersionArg,
			final GraphDatabaseService database, final TransactionHandler tx, final NamespaceIndex namespaceIndex)
			throws DMPGraphException {

		super(dataModelUri, optionalVersionArg, database, tx, namespaceIndex, TYPE);

		recordUri = recordUriArg;
	}

	@Override
	protected Node getResourceNode() throws DMPGraphException {

		final long resourceUriDataModelUriHash = HashUtils.generateHash(recordUri + dataModelUri);

		return database.findNode(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.HASH, resourceUriDataModelUriHash);
	}
}
