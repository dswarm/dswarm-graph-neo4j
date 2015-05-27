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
package org.dswarm.graph.delta.match;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.mark.Marker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 * @param <ENTITY>
 */
public abstract class Matcher<ENTITY> implements MatchResultSet<ENTITY> {

	private static final Logger LOG = LoggerFactory.getLogger(Matcher.class);

	protected Set<String> matches;
	protected boolean matchesCalculated = false;

	protected final Optional<Map<String, ENTITY>> existingEntities;
	protected final Optional<Map<String, ENTITY>> newEntities;

	protected final GraphDatabaseService existingResourceDB;
	protected final GraphDatabaseService newResourceDB;

	protected final String prefixedExistingResourceURI;
	protected final String prefixedNewResourceURI;

	protected final Marker<ENTITY> marker;

	public Matcher(final Optional<? extends Collection<ENTITY>> existingEntitiesArg, final Optional<? extends Collection<ENTITY>> newEntitiesArg,
			final GraphDatabaseService existingResourceDBArg, final GraphDatabaseService newResourceDBArg, final String prefixedExistingResourceURIArg,
			final String prefixedNewResourceURIArg, final Marker<ENTITY> markerArg) throws DMPGraphException {

		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;

		prefixedExistingResourceURI = prefixedExistingResourceURIArg;
		prefixedNewResourceURI = prefixedNewResourceURIArg;

		marker = markerArg;

		if (existingEntitiesArg.isPresent()) {

			existingEntities = Optional.fromNullable(generateHashes(existingEntitiesArg.get(), existingResourceDB));
		} else {

			existingEntities = Optional.absent();
		}

		if (newEntitiesArg.isPresent()) {

			newEntities = Optional.fromNullable(generateHashes(newEntitiesArg.get(), newResourceDB));
		} else {

			newEntities = Optional.absent();
		}
	}

	protected abstract Map<String, ENTITY> generateHashes(final Collection<ENTITY> entities, final GraphDatabaseService resourceDB)
			throws DMPGraphException;

	@Override
	public void match() throws DMPGraphException {

		getMatches();
		markMatchedPaths();
	}

	public Optional<? extends Collection<ENTITY>> getExistingEntitiesNonMatches() {

		return getNonMatches(existingEntities);
	}

	public Optional<? extends Collection<ENTITY>> getNewEntitiesNonMatches() {

		return getNonMatches(newEntities);
	}

	protected Optional<? extends Collection<String>> getMatches() {

		calculateMatches();

		return Optional.fromNullable(matches);
	}

	protected Optional<Map<String, ENTITY>> getExistingEntities() {

		return existingEntities;
	}

	protected Optional<Map<String, ENTITY>> getNewEntities() {

		return newEntities;
	}

	protected Optional<? extends Collection<ENTITY>> getMatches(final Optional<Map<String, ENTITY>> entityMap) {

		if(matches == null || matches.isEmpty()) {

			return Optional.absent();
		}

		if(!entityMap.isPresent()) {

			return Optional.absent();
		}

		final List<ENTITY> entities = new ArrayList<>();

		for(final String match : matches) {

			if(entityMap.get().containsKey(match)) {

				entities.add(entityMap.get().get(match));
			}
		}

		return Optional.fromNullable(entities);
	}

	protected void calculateMatches() {

		if(!matchesCalculated) {

			matches = new HashSet<>();

			if(existingEntities.isPresent() && newEntities.isPresent()) {

				for (final String hash : existingEntities.get().keySet()) {

					if (newEntities.get().containsKey(hash)) {

						matches.add(hash);
					}
				}
			}

			Matcher.LOG.debug("'{}' matches",  matches.size());

			matchesCalculated = true;
		}
	}

	protected Optional<? extends Collection<ENTITY>> getNonMatches(final Optional<Map<String, ENTITY>> entityMap) {

		if(matches == null || matches.isEmpty()) {

			if(!entityMap.isPresent()) {

				return Optional.absent();
			}

			return Optional.of(entityMap.get().values());
		}

		if(!entityMap.isPresent()) {

			return Optional.absent();
		}

		final List<ENTITY> valueEntities = new ArrayList<>();

		for(final Map.Entry<String, ENTITY> entityEntry : entityMap.get().entrySet()) {

			if(!matches.contains(entityEntry.getKey())) {

				valueEntities.add(entityEntry.getValue());
			}
		}

		return Optional.of(valueEntities);
	}

	protected void markMatchedPaths() throws DMPGraphException {

		Matcher.LOG.debug("mark matched paths in existing resource (exact matches)");

		markPaths(getMatches(existingEntities), DeltaState.ExactMatch, existingResourceDB, prefixedExistingResourceURI);

		Matcher.LOG.debug("mark matched paths in new resource (exact matches)");

		markPaths(getMatches(newEntities), DeltaState.ExactMatch, newResourceDB, prefixedNewResourceURI);
	}

	protected void markPaths(final Optional<? extends Collection<ENTITY>> entities, final DeltaState deltaState, final GraphDatabaseService graphDB,
			final String prefixedResourceURI) throws DMPGraphException {

		if (entities.isPresent()) {

			marker.markPaths(entities.get(), deltaState, graphDB, prefixedResourceURI);
		}
	}
}
