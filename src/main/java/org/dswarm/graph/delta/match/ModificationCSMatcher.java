package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: impl getMatches for being able to mark matches in graph
 *
 * Created by tgaengler on 01/08/14.
 */
public class ModificationCSMatcher extends ValueMatcher implements ModificationMatcher {

	public ModificationCSMatcher(final Collection<CSEntity> existingCSEntitiesArg, final Collection<CSEntity> newCSEntitiesArg) {

		super(existingCSEntitiesArg, newCSEntitiesArg);
	}

	/**
	 * hash with key + entity order + value order
	 *
	 * @param csEntities
	 * @return
	 */
	@Override
	protected Map<String, ValueEntity> generateHashes(Collection<CSEntity> csEntities) {

		final Map<String, ValueEntity> hashedValueEntities = new HashMap<>();

		for(final CSEntity csEntity : csEntities) {

			final int keyHash = csEntity.getKey().hashCode();
			final int entityOrderHash = Long.valueOf(csEntity.getEntityOrder()).hashCode();

			for(final ValueEntity valueEntity : csEntity.getValueEntities()) {

				long valueHash = keyHash;
				valueHash = 31 * valueHash +  Long.valueOf(valueEntity.getOrder()).hashCode();
				valueHash = 31 * valueHash + entityOrderHash;

				hashedValueEntities.put(Long.valueOf(valueHash).toString(), valueEntity);
			}

		}

		return hashedValueEntities;
	}

	@Override public Map<ValueEntity, ValueEntity> getModifications() {

		final Map<ValueEntity, ValueEntity> modifications = new HashMap<>();

		for(final Map.Entry<String, ValueEntity> existingValueEntityEntry : existingValueEntities.entrySet()) {

			if(newValueEntities.containsKey(existingValueEntityEntry.getKey())) {

				final ValueEntity existingValueEntity = existingValueEntityEntry.getValue();
				final ValueEntity newValueEntity = newValueEntities.get(existingValueEntityEntry.getKey());

				if(existingValueEntity.getValue() != null && newValueEntity.getValue() != null && !existingValueEntity.getValue().equals(newValueEntity.getValue())) {

					modifications.put(existingValueEntity, newValueEntity);
				}
			}
		}

		return modifications;
	}
}
