package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.dswarm.graph.delta.match.model.ValueEntity;

/**
 * @author tgaengler
 */
public abstract class ModificationCSValueMatcher extends ValueMatcher implements ModificationResultSet {

	public ModificationCSValueMatcher(final Collection<ValueEntity> existingValueEntitiesArg, final Collection<ValueEntity> newValueEntitiesArg) {

		super(existingValueEntitiesArg, newValueEntitiesArg);
	}

	@Override public Map<ValueEntity, ValueEntity> getModifications() {

		final Map<ValueEntity, ValueEntity> modifications = new HashMap<>();
		matches = new HashSet<>();

		for (final Map.Entry<String, ValueEntity> existingValueEntityEntry : existingEntities.entrySet()) {

			if (newEntities.containsKey(existingValueEntityEntry.getKey())) {

				final ValueEntity existingValueEntity = existingValueEntityEntry.getValue();
				final ValueEntity newValueEntity = newEntities.get(existingValueEntityEntry.getKey());

				if(existingValueEntity.getValue() != null && newValueEntity.getValue() != null && !existingValueEntity.getValue().equals(newValueEntity.getValue())) {

					modifications.put(existingValueEntity, newValueEntity);
					matches.add(existingValueEntityEntry.getKey());
				}
			}
		}

		return modifications;
	}

	@Override
	public Collection<String> getMatches() {

		return matches;
	}
}
