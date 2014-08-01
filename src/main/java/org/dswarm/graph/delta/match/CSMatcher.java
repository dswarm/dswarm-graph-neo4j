package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public abstract class CSMatcher {

	protected final Map<String, CSEntity> existingCSEntities;
	protected final Map<String, CSEntity> newCSEntities;

	public CSMatcher(final Collection<CSEntity> existingCSEntitiesArg, final Collection<CSEntity> newCSEntitiesArg) {

		existingCSEntities = generateHashes(existingCSEntitiesArg);
		newCSEntities = generateHashes(newCSEntitiesArg);
	}

	protected abstract Map<String, CSEntity> generateHashes(final Collection<CSEntity> csEntities);

	public Map<String, CSEntity> getExistingCSEntities() {

		return existingCSEntities;
	}

	public Map<String, CSEntity> getNewCSEntities() {

		return newCSEntities;
	}
}

