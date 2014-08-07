package org.dswarm.graph.delta.match;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author tgaengler
 *
 * @param <ENTITY>
 */
public abstract class Matcher<ENTITY> implements MatchResultSet {

	protected Set<String>				matches;

	protected Map<String, ENTITY>	existingEntities;
	protected Map<String, ENTITY>	newEntities;

	public Matcher(final Collection<ENTITY> existingEntitiesArg, final Collection<ENTITY> newEntitiesArg) {

		existingEntities = generateHashes(existingEntitiesArg);
		newEntities = generateHashes(newEntitiesArg);
	}

	protected abstract Map<String, ENTITY> generateHashes(final Collection<ENTITY> entities);

	public Map<String, ENTITY> getExistingEntities() {

		return existingEntities;
	}

	public Map<String, ENTITY> getNewEntities() {

		return newEntities;
	}

	@Override
	public Collection<String> getMatches() {

		matches = new HashSet<>();

		for (final String hash : existingEntities.keySet()) {

			if (newEntities.containsKey(hash)) {

				matches.add(hash);
			}
		}

		return matches;
	}

	public Collection<ENTITY> getMatches(final Map<String, ENTITY> entityMap) {

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

	public Collection<ENTITY> getNonMatches(final Map<String, ENTITY> entityMap) {

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
}
