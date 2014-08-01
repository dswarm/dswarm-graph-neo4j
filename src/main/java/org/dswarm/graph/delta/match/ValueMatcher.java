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

	public ValueMatcher(final Collection<CSEntity> existingCSEntitiesArg, final Collection<CSEntity> newCSEntitiesArg) {

		existingValueEntities = generateHashes(existingCSEntitiesArg);
		newValueEntities = generateHashes(newCSEntitiesArg);
	}

	protected abstract Map<String, ValueEntity> generateHashes(final Collection<CSEntity> csEntities);

	public Map<String, ValueEntity> getExistingValueEntities() {

		return existingValueEntities;
	}

	public Map<String, ValueEntity> getNewValueEntities() {

		return newValueEntities;
	}
}
