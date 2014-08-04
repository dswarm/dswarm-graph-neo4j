package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public abstract class ValueMatcher {

	protected final Map<String, ValueEntity> existingValueEntities;
	protected final Map<String, ValueEntity> newValueEntities;

	public ValueMatcher(final Collection<ValueEntity> existingValueEntitiesArg, final Collection<ValueEntity> newValueEntitiesArg) {

		existingValueEntities = generateHashes(existingValueEntitiesArg);
		newValueEntities = generateHashes(newValueEntitiesArg);
	}

	protected abstract Map<String, ValueEntity> generateHashes(final Collection<ValueEntity> valueEntities);

	public Map<String, ValueEntity> getExistingValueEntities() {

		return existingValueEntities;
	}

	public Map<String, ValueEntity> getNewValueEntities() {

		return newValueEntities;
	}
}
