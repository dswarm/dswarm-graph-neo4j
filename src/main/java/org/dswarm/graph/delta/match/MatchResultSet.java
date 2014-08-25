package org.dswarm.graph.delta.match;

import java.util.Collection;

/**
 * Created by tgaengler on 01/08/14.
 */
public interface MatchResultSet<ENTITY> {

	void match();

	Collection<ENTITY> getExistingEntitiesNonMatches();

	Collection<ENTITY> getNewEntitiesNonMatches();
}
