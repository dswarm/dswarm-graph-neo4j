package org.dswarm.graph.delta.util;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;

/**
 * @author tgaengler
 */
public final class GraphDBMarkUtil {

	private static final Logger LOG = LoggerFactory.getLogger(GraphDBMarkUtil.class);

	public static void markPaths(final DeltaState deltaState, final GraphDatabaseService graphDB, final String resourceURI,
			final Set<Long> pathEndNodeIds) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Path> paths = GraphDBUtil.getResourcePaths(graphDB, resourceURI);

			markPaths(deltaState, pathEndNodeIds, paths);

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBMarkUtil.LOG.error("couldn't mark paths successfully", e);
		} finally {

			tx.close();
		}
	}

	public static void markPaths(final DeltaState deltaState, final GraphDatabaseService graphDB, final long nodeId, final Set<Long> pathEndNodeIds) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Path> paths = GraphDBUtil.getEntityPaths(graphDB, nodeId);

			markPaths(deltaState, pathEndNodeIds, paths);

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBMarkUtil.LOG.error("couldn't mark paths successfully", e);
		} finally {

			tx.close();
		}
	}

	private static void markPaths(final DeltaState deltaState, final Set<Long> pathEndNodeIds, final Iterable<Path> paths) {

		for (final Path path : paths) {

			final long pathEndNodeId = path.endNode().getId();

			if (pathEndNodeIds.contains(pathEndNodeId)) {

				// mark path
				for (final Relationship rel : path.relationships()) {

					if (!rel.hasProperty(DeltaStatics.DELTA_STATE_PROPERTY)) {

						rel.setProperty(DeltaStatics.DELTA_STATE_PROPERTY, deltaState.toString());
					}

					rel.setProperty(DeltaStatics.MATCHED_PROPERTY, true);
				}

				for (final Node node : path.nodes()) {

					if (!node.hasProperty(DeltaStatics.DELTA_STATE_PROPERTY)) {

						node.setProperty(DeltaStatics.DELTA_STATE_PROPERTY, deltaState.toString());
					} else if (deltaState.equals(DeltaState.ExactMatch)) {

						final String deltaStateString = (String) node.getProperty(DeltaStatics.DELTA_STATE_PROPERTY);
						final DeltaState currentDeltaState = DeltaState.getByName(deltaStateString);

						switch (currentDeltaState) {

							case ADDITION:
							case DELETION:
							case MODIFICATION:

								// modify delta state if a "higher" delta state was determined
								node.setProperty(DeltaStatics.DELTA_STATE_PROPERTY, deltaState.toString());

								break;
						}
					}

					node.setProperty(DeltaStatics.MATCHED_PROPERTY, true);
				}
			}
		}
	}

	/**
	 * note: we may need to find a better way to handle those statements
	 *
	 * @param graphDB
	 * @param nodeId
	 */
	private static void markEntityTypeNodes(final GraphDatabaseService graphDB, final DeltaState deltaState, final long nodeId) {

		final Set<Long> pathEndNodeIds = new HashSet<>();

		GraphDBUtil.fetchEntityTypeNodes(graphDB, pathEndNodeIds, nodeId);
		markPaths(deltaState, graphDB, nodeId, pathEndNodeIds);
	}
}
