package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.model.ValueEntity;

/**
 * @author tgaengler
 */
public class FirstDegreeModificationCSValueMatcher extends ModificationCSValueMatcher {

	public FirstDegreeModificationCSValueMatcher(final Collection<ValueEntity> existingValueEntitiesArg,
			final Collection<ValueEntity> newValueEntitiesArg) {

		super(existingValueEntitiesArg, newValueEntitiesArg);
	}

	/**
	 * hash with key + entity order + value order
	 *
	 * @param valueEntities
	 * @return
	 */
	@Override
	protected Map<String, ValueEntity> generateHashes(Collection<ValueEntity> valueEntities) {

		final Map<String, ValueEntity> hashedValueEntities = new HashMap<>();

		for(final ValueEntity valueEntity : valueEntities) {

			final int keyHash = valueEntity.getCSEntity().getKey().hashCode();
			final int entityOrderHash = Long.valueOf(valueEntity.getCSEntity().getEntityOrder()).hashCode();

			long valueHash = keyHash;
			valueHash = 31 * valueHash +  Long.valueOf(valueEntity.getOrder()).hashCode();
			valueHash = 31 * valueHash + entityOrderHash;

			hashedValueEntities.put(Long.valueOf(valueHash).toString(), valueEntity);
		}

		return hashedValueEntities;
	}
}
