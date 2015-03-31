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
package org.dswarm.graph.versioning.utils;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
public final class GraphVersionUtils {

	/**
	 * note: must be in transaction scope
	 *
	 * @param dataModelUri
	 * @param database
	 * @return
	 */
	public static int getLatestVersion(final String dataModelUri, final GraphDatabaseService database) {

		int latestVersion = 1;

		final Index<Node> resources = database.index().forNodes(GraphIndexStatics.RESOURCES_INDEX_NAME);
		final IndexHits<Node> hits = resources.get(GraphStatics.URI, dataModelUri);

		if (hits != null && hits.iterator().hasNext()) {

			final Node dataModelNode = hits.iterator().next();
			final Integer latestVersionFromDB = (Integer) dataModelNode.getProperty(VersioningStatics.LATEST_VERSION_PROPERTY, null);

			if (latestVersionFromDB != null) {

				latestVersion = latestVersionFromDB;
			}
		}

		if (hits != null) {

			hits.close();
		}

		return latestVersion;
	}
}
