package org.dswarm.graph.delta.match.mark;

import java.util.Collection;

import org.dswarm.graph.delta.DeltaState;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 * @param <ENTITY>
 */
public interface Marker<ENTITY> {

	void markPaths(final Collection<ENTITY> entities, final DeltaState deltaState, final GraphDatabaseService graphDB, final String resourceURI);

}
