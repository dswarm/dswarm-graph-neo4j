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
			final String resourceURI) throws DMPGraphException {

		final Set<Long> pathEndNodeIds = new HashSet<>();
		final Map<CSEntity, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();
		final Map<CSEntity, Set<Long>> modifiedPathEndNodesIdsFromCSEntityMap = new HashMap<>();

		try (final Transaction tx = graphDB.beginTx()) {

			// calc path end nodes
			for (final CSEntity csEntity : csEntities) {

				// TODO: mark type nodes from other paths as well?

				for (final KeyEntity keyEntity : csEntity.getKeyEntities()) {

					pathEndNodeIds.add(keyEntity.getNodeId());
				}

				for (final ValueEntity valueEntity : csEntity.getValueEntities()) {

					if(!deltaState.equals(DeltaState.MODIFICATION)) {

						pathEndNodeIds.add(valueEntity.getNodeId());
					} else {

						if(!modifiedPathEndNodesIdsFromCSEntityMap.containsKey(csEntity)) {

							modifiedPathEndNodesIdsFromCSEntityMap.put(csEntity, new HashSet<Long>());
						}

						modifiedPathEndNodesIdsFromCSEntityMap.get(csEntity).add(valueEntity.getNodeId());
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

		GraphDBMarkUtil.markPaths(finalDeltaState, graphDB, resourceURI, pathEndNodeIds);

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
