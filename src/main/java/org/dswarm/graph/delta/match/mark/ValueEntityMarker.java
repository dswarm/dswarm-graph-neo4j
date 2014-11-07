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
public class ValueEntityMarker implements Marker<ValueEntity> {

	private static final Logger	LOG	= LoggerFactory.getLogger(ValueEntityMarker.class);

	public void markPaths(final Collection<ValueEntity> valueEntities, final DeltaState deltaState, final GraphDatabaseService graphDB,
			final String resourceURI) throws DMPGraphException {

		final Set<Long> pathEndNodeIds = new HashSet<>();
		final Set<Long> modifiedPathEndNodeIds = new HashSet<>();
		final Map<CSEntity, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();
		final Map<CSEntity, Set<Long>> modifiedPathEndNodesIdsFromCSEntityMap = new HashMap<>();

		try(final Transaction tx = graphDB.beginTx()) {

			// calc path end nodes
			for (final ValueEntity valueEntity : valueEntities) {

				// TODO: what should I do with related key paths here?
				for (final KeyEntity keyEntity : valueEntity.getCSEntity().getKeyEntities()) {

					GraphDBUtil.addNodeId(pathEndNodeIds, keyEntity.getNodeId());
				}

				final Long csEntityNodeId = valueEntity.getCSEntity().getNodeId();

				if(!deltaState.equals(DeltaState.MODIFICATION)) {

					GraphDBUtil.addNodeId(pathEndNodeIds, valueEntity.getNodeId());
				} else if(csEntityNodeId != null && csEntityNodeId >= 0) {

					if(!modifiedPathEndNodesIdsFromCSEntityMap.containsKey(valueEntity.getCSEntity())) {

						modifiedPathEndNodesIdsFromCSEntityMap.put(valueEntity.getCSEntity(), new HashSet<Long>());
					}

					GraphDBUtil.addNodeId(modifiedPathEndNodesIdsFromCSEntityMap.get(valueEntity.getCSEntity()), valueEntity.getNodeId());
				} else {

					GraphDBUtil.addNodeId(modifiedPathEndNodeIds, valueEntity.getNodeId());
				}

				if(csEntityNodeId != null && csEntityNodeId >= 0) {

					final Set<Long> pathEndNodeIdsFromCSEntity;

					if(pathEndNodesIdsFromCSEntityMap.containsKey(valueEntity.getCSEntity())) {

						pathEndNodeIdsFromCSEntity = pathEndNodesIdsFromCSEntityMap.get(valueEntity.getCSEntity());
					} else {

						pathEndNodeIdsFromCSEntity = new HashSet<>();
					}

					// TODO: could be removed later
					ValueEntityMarker.LOG.debug("fetch entity type nodes in value entity marker");

					GraphDBUtil.fetchEntityTypeNodes(graphDB, pathEndNodeIdsFromCSEntity, csEntityNodeId);
					// TODO: we can't do this here, or? - I think we'll probably mark too much
					GraphDBUtil.determineNonMatchedSubGraphPathEndNodes(deltaState, graphDB, pathEndNodeIdsFromCSEntity, csEntityNodeId);

					pathEndNodesIdsFromCSEntityMap.put(valueEntity.getCSEntity(), pathEndNodeIdsFromCSEntity);
				}
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't identify paths for marking successfully";

			ValueEntityMarker.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		final DeltaState finalDeltaState;

		if(!deltaState.equals(DeltaState.MODIFICATION)) {

			finalDeltaState = deltaState;
		} else {

			finalDeltaState = DeltaState.ExactMatch;
		}

		GraphDBMarkUtil.markPaths(finalDeltaState, graphDB, resourceURI, pathEndNodeIds);


		if(!modifiedPathEndNodeIds.isEmpty()) {

			GraphDBMarkUtil.markPaths(deltaState, graphDB, resourceURI, modifiedPathEndNodeIds);
		}

		for(final Map.Entry<CSEntity, Set<Long>> pathEndNideIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBMarkUtil.markPaths(finalDeltaState, graphDB, pathEndNideIdsFromCSEntityEntry.getKey().getNodeId(),
					pathEndNideIdsFromCSEntityEntry.getValue());
		}

		for(final Map.Entry<CSEntity, Set<Long>> modifiedPathEndNodeIdsFromCSEntityEntry : modifiedPathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBMarkUtil.markPaths(deltaState, graphDB, modifiedPathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(),
					modifiedPathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}
}
