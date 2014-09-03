package org.dswarm.graph.delta.match;

import java.util.Collection;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 *
 * @param <ENTITY>
 */
public interface MatchResultSet<ENTITY> {

	void match();

	Optional<? extends Collection<ENTITY>> getExistingEntitiesNonMatches();

	Optional<? extends Collection<ENTITY>> getNewEntitiesNonMatches();
}
