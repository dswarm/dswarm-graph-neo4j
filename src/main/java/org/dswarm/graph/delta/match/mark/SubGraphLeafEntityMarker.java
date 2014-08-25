package org.dswarm.graph.delta.match.mark;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public class SubGraphLeafEntityMarker implements Marker<SubGraphLeafEntity> {

	@Override public void markPaths(final Collection<SubGraphLeafEntity> subGraphLeafEntities, final DeltaState deltaState,
			final GraphDatabaseService graphDB, final String resourceURI) {

		final Map<Long, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();

		for(final SubGraphLeafEntity subGraphLeafEntity : subGraphLeafEntities) {

			if(!pathEndNodesIdsFromCSEntityMap.containsKey(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getNodeId())) {

				pathEndNodesIdsFromCSEntityMap.put(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getNodeId(), new HashSet<Long>());
			}

			pathEndNodesIdsFromCSEntityMap.get(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getNodeId()).add(subGraphLeafEntity.getNodeId());
		}

		for(final Map.Entry<Long, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBUtil.markPaths(deltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey(), pathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}
}
