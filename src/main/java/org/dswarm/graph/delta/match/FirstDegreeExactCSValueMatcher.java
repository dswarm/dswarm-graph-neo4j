package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

/**
 * @author tgaengler
 */
public class FirstDegreeExactCSValueMatcher extends ValueMatcher {

	public FirstDegreeExactCSValueMatcher(final Collection<ValueEntity> existingValueEntitiesArg, final Collection<ValueEntity> newValueEntitiesArg) {

		super(existingValueEntitiesArg, newValueEntitiesArg);
	}

	/**
	 * hash with key, value + entity order + value order => matches value entities
	 * 
	 * @param valueEntities
	 * @return
	 */
	@Override protected Map<String, ValueEntity> generateHashes(final Collection<ValueEntity> valueEntities) {
		
		final Map<String, ValueEntity> hashedValueEntities = new HashMap<>();


		for(final ValueEntity valueEntity : valueEntities) {

			final int keyHash = valueEntity.getCSEntity().getKey().hashCode();
			final int entityOrderHash = Long.valueOf(valueEntity.getCSEntity().getEntityOrder()).hashCode();

			long valueHash = keyHash;
			valueHash = 31 * valueHash + valueEntity.getValue().hashCode();
			valueHash = 31 * valueHash +  Long.valueOf(valueEntity.getOrder()).hashCode();
			valueHash = 31 * valueHash + entityOrderHash;

			hashedValueEntities.put(Long.valueOf(valueHash).toString(), valueEntity);
		}

		return hashedValueEntities;
	}
}
