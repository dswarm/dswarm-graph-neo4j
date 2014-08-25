package org.dswarm.graph.delta.match;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.mark.Marker;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 * @param <ENTITY>
 */
public abstract class Matcher<ENTITY> implements MatchResultSet<ENTITY> {

	protected Set<String>					matches;
	protected boolean						matchesCalculated	= false;

	protected final Map<String, ENTITY>		existingEntities;
	protected final Map<String, ENTITY>		newEntities;

	protected final GraphDatabaseService	existingResourceDB;
	protected final GraphDatabaseService	newResourceDB;

	protected final String					existingResourceURI;
	protected final String					newResourceURI;

	protected final Marker<ENTITY>			marker;

	public Matcher(final Collection<ENTITY> existingEntitiesArg, final Collection<ENTITY> newEntitiesArg,
			final GraphDatabaseService existingResourceDBArg, final GraphDatabaseService newResourceDBArg, final String existingResourceURIArg,
			final String newResourceURIArg, final Marker<ENTITY> markerArg) {

		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;

		existingResourceURI = existingResourceURIArg;
		newResourceURI = newResourceURIArg;

		marker = markerArg;

		existingEntities = generateHashes(existingEntitiesArg, existingResourceDB);
		newEntities = generateHashes(newEntitiesArg, newResourceDB);
	}

	protected abstract Map<String, ENTITY> generateHashes(final Collection<ENTITY> entities, final GraphDatabaseService resourceDB);

	@Override
	public void match() {

		getMatches();
		markMatchedPaths();
	}

	public Collection<ENTITY> getExistingEntitiesNonMatches() {

		return getNonMatches(existingEntities);
	}

	public Collection<ENTITY> getNewEntitiesNonMatches() {

		return getNonMatches(newEntities);
	}

	protected Collection<String> getMatches() {

		calculateMatches();

		return matches;
	}

	protected Map<String, ENTITY> getExistingEntities() {

		return existingEntities;
	}

	protected Map<String, ENTITY> getNewEntities() {

		return newEntities;
	}

	protected Collection<ENTITY> getMatches(final Map<String, ENTITY> entityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<ENTITY> entities = new ArrayList<>();

		for(final String match : matches) {

			if(entityMap.containsKey(match)) {

				entities.add(entityMap.get(match));
			}
		}

		return entities;
	}

	protected void calculateMatches() {

		if(!matchesCalculated) {

			matches = new HashSet<>();

			for (final String hash : existingEntities.keySet()) {

				if (newEntities.containsKey(hash)) {

					matches.add(hash);
				}
			}

			matchesCalculated = true;
		}
	}

	protected Collection<ENTITY> getNonMatches(final Map<String, ENTITY> entityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<ENTITY> valueEntities = new ArrayList<>();

		for(final Map.Entry<String, ENTITY> entityEntry : entityMap.entrySet()) {

			if(!matches.contains(entityEntry.getKey())) {

				valueEntities.add(entityEntry.getValue());
			}
		}

		return valueEntities;
	}

	protected void markMatchedPaths() {

		markPaths(getMatches(existingEntities), DeltaState.ExactMatch, existingResourceDB, existingResourceURI);
		markPaths(getMatches(newEntities), DeltaState.ExactMatch, newResourceDB, newResourceURI);
	}

	protected void markPaths(final Collection<ENTITY> entities, final DeltaState deltaState, final GraphDatabaseService graphDB,
			final String resourceURI) {

		final Optional<Collection<ENTITY>> optionalEntities = Optional.fromNullable(entities);

		if (optionalEntities.isPresent()) {

			marker.markPaths(optionalEntities.get(), deltaState, graphDB, resourceURI);
		}
	}
}
