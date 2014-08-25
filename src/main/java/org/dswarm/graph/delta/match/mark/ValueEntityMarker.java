package org.dswarm.graph.delta.match.mark;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.KeyEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * @author tgaengler
 */
public class ValueEntityMarker implements Marker<ValueEntity> {

	public void markPaths(final Collection<ValueEntity> valueEntities, final DeltaState deltaState, final GraphDatabaseService graphDB, final String resourceURI) {

		final Set<Long> pathEndNodeIds = new HashSet<>();
		final Set<Long> modifiedPathEndNodeIds = new HashSet<>();
		final Map<CSEntity, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();
		final Map<CSEntity, Set<Long>> modifiedPathEndNodesIdsFromCSEntityMap = new HashMap<>();

		try (final Transaction ignored = graphDB.beginTx()) {

			// calc path end nodes
			for(final ValueEntity valueEntity : valueEntities) {

				// TODO: what should I do with related key paths here?
				for(final KeyEntity keyEntity : valueEntity.getCSEntity().getKeyEntities()) {

					pathEndNodeIds.add(keyEntity.getNodeId());
				}

				final long csEntityNodeId = valueEntity.getCSEntity().getNodeId();

				if(!deltaState.equals(DeltaState.MODIFICATION)) {

					pathEndNodeIds.add(valueEntity.getNodeId());
				} else if(csEntityNodeId >= 0) {

					if(!modifiedPathEndNodesIdsFromCSEntityMap.containsKey(valueEntity.getCSEntity())) {

						modifiedPathEndNodesIdsFromCSEntityMap.put(valueEntity.getCSEntity(), new HashSet<Long>());
					}

					modifiedPathEndNodesIdsFromCSEntityMap.get(valueEntity.getCSEntity()).add(valueEntity.getNodeId());
				} else {

					modifiedPathEndNodeIds.add(valueEntity.getNodeId());
				}

				if(csEntityNodeId >= 0) {


					final Set<Long> pathEndNodeIdsFromCSEntity;

					if(pathEndNodesIdsFromCSEntityMap.containsKey(valueEntity.getCSEntity())) {

						pathEndNodeIdsFromCSEntity = pathEndNodesIdsFromCSEntityMap.get(valueEntity.getCSEntity());
					} else {

						pathEndNodeIdsFromCSEntity = new HashSet<>();
					}

					GraphDBUtil.fetchEntityTypeNodes(graphDB, pathEndNodeIdsFromCSEntity, csEntityNodeId);
					// TODO: we can't do this here, or? - I think we'll probably mark too much
					GraphDBUtil.determineNonMatchedSubGraphPathEndNodes(deltaState, graphDB, pathEndNodeIdsFromCSEntity, csEntityNodeId);

					pathEndNodesIdsFromCSEntityMap.put(valueEntity.getCSEntity(), pathEndNodeIdsFromCSEntity);
				}
			}
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		final DeltaState finalDeltaState;

		if(!deltaState.equals(DeltaState.MODIFICATION)) {

			finalDeltaState = deltaState;
		} else {

			finalDeltaState = DeltaState.ExactMatch;
		}

		GraphDBUtil.markPaths(finalDeltaState, graphDB, resourceURI, pathEndNodeIds);


		if(!modifiedPathEndNodeIds.isEmpty()) {

			GraphDBUtil.markPaths(deltaState, graphDB, resourceURI, modifiedPathEndNodeIds);
		}

		for(final Map.Entry<CSEntity, Set<Long>> pathEndNideIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBUtil.markPaths(finalDeltaState, graphDB, pathEndNideIdsFromCSEntityEntry.getKey().getNodeId(),
					pathEndNideIdsFromCSEntityEntry.getValue());
		}

		for(final Map.Entry<CSEntity, Set<Long>> modifiedPathEndNodeIdsFromCSEntityEntry : modifiedPathEndNodesIdsFromCSEntityMap.entrySet()) {

			GraphDBUtil.markPaths(deltaState, graphDB, modifiedPathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(),
					modifiedPathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}
}
