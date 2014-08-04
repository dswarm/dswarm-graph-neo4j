package org.dswarm.graph.delta.match;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

/**
 * @author tgaengler
 */
public abstract class CSValueMatcher extends ValueMatcher implements MatchResultSet {

	protected Set<String> matches;

	public CSValueMatcher(final Collection<ValueEntity> existingValueEntitiesArg, final Collection<ValueEntity> newValueEntitiesArg) {

		super(existingValueEntitiesArg, newValueEntitiesArg);
	}

	public Collection<ValueEntity> getMatches(final Map<String, ValueEntity> valueEntityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<ValueEntity> valueEntities = new ArrayList<>();

		for(final String match : matches) {

			if(valueEntityMap.containsKey(match)) {

				valueEntities.add(valueEntityMap.get(match));
			}
		}

		return valueEntities;
	}

	public Collection<ValueEntity> getNonMatches(final Map<String, ValueEntity> valueEntityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<ValueEntity> valueEntities = new ArrayList<>();

		for(final Map.Entry<String, ValueEntity> valueEntityEntry : valueEntityMap.entrySet()) {

			if(!matches.contains(valueEntityEntry.getKey())) {

				valueEntities.add(valueEntityEntry.getValue());
			}
		}

		return valueEntities;
	}
}
