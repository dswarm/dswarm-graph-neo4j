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
package org.dswarm.graph.delta.match.mark;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.util.GraphDBMarkUtil;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class SubGraphEntityMarker implements Marker<SubGraphEntity> {

	private static final Logger	LOG	= LoggerFactory.getLogger(SubGraphEntityMarker.class);

	@Override public void markPaths(final Collection<SubGraphEntity> subGraphEntities, final DeltaState deltaState,
			final GraphDatabaseService graphDB, final String resourceURI) throws DMPGraphException {

		final Map<Long, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();

		// calc path end nodes
		for (final SubGraphEntity subGraphEntity : subGraphEntities) {

			final Collection<String> leafNodes = GraphDBUtil.getEntityLeafs(graphDB, subGraphEntity.getNodeId());

			if (leafNodes != null && !leafNodes.isEmpty()) {

				final Set<Long> pathEndNodeIds;

				if (pathEndNodesIdsFromCSEntityMap.containsKey(subGraphEntity.getCSEntity().getNodeId())) {

					pathEndNodeIds = pathEndNodesIdsFromCSEntityMap.get(subGraphEntity.getCSEntity().getNodeId());
				} else {

					pathEndNodeIds = new HashSet<>();
				}

				for(final String leafNode : leafNodes) {

					GraphDBUtil.addNodeId(pathEndNodeIds, Long.valueOf(leafNode));
				}

				pathEndNodesIdsFromCSEntityMap.put(subGraphEntity.getCSEntity().getNodeId(), pathEndNodeIds);
			}
		}

		for(final Map.Entry<Long, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBMarkUtil.markPaths(deltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey(), pathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}
}
