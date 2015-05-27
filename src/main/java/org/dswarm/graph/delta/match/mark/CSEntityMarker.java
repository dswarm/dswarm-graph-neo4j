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
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.KeyEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.delta.util.GraphDBMarkUtil;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class CSEntityMarker implements Marker<CSEntity> {

	private static final Logger LOG = LoggerFactory.getLogger(CSEntityMarker.class);

	@Override
	public void markPaths(final Collection<CSEntity> csEntities, final DeltaState deltaState, final GraphDatabaseService graphDB,
			final String prefixedResourceURI) throws DMPGraphException {

		final Set<Long> pathEndNodeIds = new HashSet<>();
		final Map<CSEntity, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();
		final Map<CSEntity, Set<Long>> modifiedPathEndNodesIdsFromCSEntityMap = new HashMap<>();

		try (final Transaction tx = graphDB.beginTx()) {

			// calc path end nodes
			for (final CSEntity csEntity : csEntities) {

				// TODO: mark type nodes from other paths as well?

				for (final KeyEntity keyEntity : csEntity.getKeyEntities()) {

					GraphDBUtil.addNodeId(pathEndNodeIds, keyEntity.getNodeId());
				}

				for (final ValueEntity valueEntity : csEntity.getValueEntities()) {

					if(!deltaState.equals(DeltaState.MODIFICATION)) {

						GraphDBUtil.addNodeId(pathEndNodeIds, valueEntity.getNodeId());
					} else {

						if(!modifiedPathEndNodesIdsFromCSEntityMap.containsKey(csEntity)) {

							modifiedPathEndNodesIdsFromCSEntityMap.put(csEntity, new HashSet<Long>());
						}

						GraphDBUtil.addNodeId(modifiedPathEndNodesIdsFromCSEntityMap.get(csEntity), valueEntity.getNodeId());
					}
				}

				final Set<Long> pathEndNodeIdsFromCSEntity = new HashSet<>();

				// TODO: could be removed later
				CSEntityMarker.LOG.debug("fetch entity type nodes in cs entity marker");

				//markEntityTypeNodes(graphDB, deltaState, csEntity.getNodeId());
				GraphDBUtil.fetchEntityTypeNodes(graphDB, pathEndNodeIdsFromCSEntity, csEntity.getNodeId());

				GraphDBUtil.determineNonMatchedSubGraphPathEndNodes(deltaState, graphDB, pathEndNodeIdsFromCSEntity, csEntity.getNodeId());

				pathEndNodesIdsFromCSEntityMap.put(csEntity, pathEndNodeIdsFromCSEntity);
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't identify paths for marking successfully";

			CSEntityMarker.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		final DeltaState finalDeltaState;

		if(!deltaState.equals(DeltaState.MODIFICATION)) {

			finalDeltaState = deltaState;
		} else {

			finalDeltaState = DeltaState.ExactMatch;
		}

		GraphDBMarkUtil.markPaths(finalDeltaState, graphDB, prefixedResourceURI, pathEndNodeIds);

		for(final Map.Entry<CSEntity, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBMarkUtil.markPaths(finalDeltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(),
					pathEndNodeIdsFromCSEntityEntry.getValue());
		}

		for(final Map.Entry<CSEntity, Set<Long>> modifiedPathEndNodeIdsFromCSEntityEntry : modifiedPathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBMarkUtil.markPaths(deltaState, graphDB, modifiedPathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(),
					modifiedPathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}
}
