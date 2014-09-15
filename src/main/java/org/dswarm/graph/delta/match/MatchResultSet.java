package org.dswarm.graph.delta.match;

import java.util.Collection;

import com.google.common.base.Optional;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 *
 * @param <ENTITY>
 */
public interface MatchResultSet<ENTITY> {

	void match() throws DMPGraphException;

	Optional<? extends Collection<ENTITY>> getExistingEntitiesNonMatches();

	Optional<? extends Collection<ENTITY>> getNewEntitiesNonMatches();
}
