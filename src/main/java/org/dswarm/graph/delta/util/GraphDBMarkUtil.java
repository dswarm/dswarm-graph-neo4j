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
package org.dswarm.graph.delta.util;

import java.util.HashSet;
import java.util.Set;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * @author tgaengler
 */
public final class GraphDBMarkUtil {

	private static final Logger	LOG	= LoggerFactory.getLogger(GraphDBMarkUtil.class);

	public static void markPaths(final DeltaState deltaState, final GraphDatabaseService graphDB, final String resourceURI,
			final Set<Long> pathEndNodeIds) throws DMPGraphException {

		try(final Transaction tx = graphDB.beginTx()) {

			final Iterable<Path> paths = GraphDBUtil.getResourcePaths(graphDB, resourceURI);

			markPaths(deltaState, pathEndNodeIds, paths);

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't mark paths successfully";

			GraphDBMarkUtil.LOG.error(message, e);

			throw new DMPGraphException(message, e);
		}
	}

	public static void markPaths(final DeltaState deltaState, final GraphDatabaseService graphDB, final long nodeId, final Set<Long> pathEndNodeIds)
			throws DMPGraphException {

		try(final Transaction tx = graphDB.beginTx()) {

			final Iterable<Path> paths = GraphDBUtil.getEntityPaths(graphDB, nodeId);

			markPaths(deltaState, pathEndNodeIds, paths);

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't mark paths successfully";

			GraphDBMarkUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	private static void markPaths(final DeltaState deltaState, final Set<Long> pathEndNodeIds, final Iterable<Path> paths) {

		final Set<Long> markedPathEndNodeIds = Sets.newHashSet();

		for (final Path path : paths) {

			final long pathEndNodeId = path.endNode().getId();

			if (pathEndNodeIds.contains(pathEndNodeId)) {

				markedPathEndNodeIds.add(pathEndNodeId);

				// mark path
				for (final Relationship rel : path.relationships()) {

					if (!rel.hasProperty(DeltaStatics.DELTA_STATE_PROPERTY)) {

						rel.setProperty(DeltaStatics.DELTA_STATE_PROPERTY, deltaState.toString());
					}

					// TODO: remove this later, it'S just for debugging purpose right now
					if (rel.getType().name().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {

						GraphDBMarkUtil.LOG.debug("mark rel: " + GraphDBPrintUtil.printDeltaRelationship(rel));
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

		if (pathEndNodeIds.size() != markedPathEndNodeIds.size()) {

			GraphDBMarkUtil.LOG.error("couldn't mark all paths; path end node ids size = '" + pathEndNodeIds.size()
					+ "' :: marked path end node ids size = '" + markedPathEndNodeIds.size() + "'");

			for (final Long pathEndNodeId : pathEndNodeIds) {

				if (!markedPathEndNodeIds.contains(pathEndNodeId)) {

					GraphDBMarkUtil.LOG.error("couldn't mark path with end node id = '" + pathEndNodeId + "'");
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
	private static void markEntityTypeNodes(final GraphDatabaseService graphDB, final DeltaState deltaState, final long nodeId)
			throws DMPGraphException {

		final Set<Long> pathEndNodeIds = new HashSet<>();

		GraphDBUtil.fetchEntityTypeNodes(graphDB, pathEndNodeIds, nodeId);
		markPaths(deltaState, graphDB, nodeId, pathEndNodeIds);
	}
}
