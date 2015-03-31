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

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.model.GraphStatics;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMResourceByURIReader extends PropertyGraphGDMResourceReader {

	private static final String TYPE = "GDM record by URI";

	private final String	recordUri;

	public PropertyGraphGDMResourceByURIReader(final String recordUriArg, final String dataModelUri, final Optional<Integer> optionalVersionArg, final GraphDatabaseService database)
			throws DMPGraphException {

		super(dataModelUri, optionalVersionArg, database, TYPE);

		recordUri = recordUriArg;
	}

	@Override
	protected Node getResourceNode()  throws DMPGraphException {

		final Index<Node> resourcesWDataModel = database.index().forNodes(GraphIndexStatics.RESOURCES_W_DATA_MODEL_INDEX_NAME);
		final IndexHits<Node> hits = resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL, recordUri + dataModelUri);

		if (hits == null) {

			return null;
		}
		if (!hits.hasNext()) {

			hits.close();

			return null;
		}

		final Node node = hits.next();

		hits.close();

		return node;
	}
}
