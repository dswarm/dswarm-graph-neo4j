package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tgaengler on 01/08/14.
 */
public abstract class ExactCSMatcher extends CSEntityMatcher implements MatchResultSet {

	private Set<String> matches;

	public ExactCSMatcher(final Collection<CSEntity> existingCSEntitiesArg, final Collection<CSEntity> newCSEntitiesArg) {

		super(existingCSEntitiesArg, newCSEntitiesArg);
	}

	@Override
	public Collection<String> getMatches() {

		matches = new HashSet<>();

		for (final String hash : existingCSEntities.keySet()) {

			if (newCSEntities.containsKey(hash)) {

				matches.add(hash);
			}
		}

		return matches;
	}

	public Collection<CSEntity> getMatches(final Map<String, CSEntity> csEntityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<CSEntity> csEntities = new ArrayList<>();

		for(final String match : matches) {

			if(csEntityMap.containsKey(match)) {

				csEntities.add(csEntityMap.get(match));
			}
		}

		return csEntities;
	}

	public Collection<CSEntity> getNonMatches(final Map<String, CSEntity> csEntityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<CSEntity> csEntities = new ArrayList<>();

		for(final Map.Entry<String, CSEntity> csEntityEntry : csEntityMap.entrySet()) {

			if(!matches.contains(csEntityEntry.getKey())) {

				csEntities.add(csEntityEntry.getValue());
			}
		}

		return csEntities;
	}
}
