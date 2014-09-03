package org.dswarm.graph.delta.match.mark;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.util.GraphDBMarkUtil;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * @author tgaengler
 */
public class SubGraphEntityMarker implements Marker<SubGraphEntity> {

	@Override public void markPaths(final Collection<SubGraphEntity> subGraphEntities, final DeltaState deltaState,
			final GraphDatabaseService graphDB, final String resourceURI) {

		final Map<Long, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();

		// calc path end nodes
		for(final SubGraphEntity subGraphEntity : subGraphEntities) {

			try (final Transaction ignored = graphDB.beginTx()) {

				final Collection<String> leafNodes = GraphDBUtil.getEntityLeafs(graphDB, subGraphEntity.getNodeId());

				if(leafNodes != null && !leafNodes.isEmpty()) {

					final Set<Long> pathEndNodeIds;

					if(pathEndNodesIdsFromCSEntityMap.containsKey(subGraphEntity.getCSEntity().getNodeId())) {

						pathEndNodeIds = pathEndNodesIdsFromCSEntityMap.get(subGraphEntity.getCSEntity().getNodeId());
					} else {

						pathEndNodeIds = new HashSet<>();
					}

					for(final String leafNode : leafNodes) {

						pathEndNodeIds.add(Long.valueOf(leafNode));
					}

					pathEndNodesIdsFromCSEntityMap.put(subGraphEntity.getCSEntity().getNodeId(), pathEndNodeIds);
				}
			} catch (final Exception e) {

				// TODO: log something

				e.printStackTrace();
			}
		}

		for(final Map.Entry<Long, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBMarkUtil.markPaths(deltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey(), pathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}
}
