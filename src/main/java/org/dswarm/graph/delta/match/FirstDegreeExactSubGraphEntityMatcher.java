package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.model.SubGraphEntity;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphEntityMatcher extends SubGraphEntityMatcher {

	public FirstDegreeExactSubGraphEntityMatcher(final Collection<SubGraphEntity> existingSubGraphEntitiesArg,
			final Collection<SubGraphEntity> newSubGraphEntitiesArg) {

		super(existingSubGraphEntitiesArg, newSubGraphEntitiesArg);
	}

	/**
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph hash + order
	 *
	 * @param subGraphEntities
	 * @return
	 */
	@Override
	protected Map<String, SubGraphEntity> generateHashes(final Collection<SubGraphEntity> subGraphEntities) {

		final Map<String, SubGraphEntity> hashedCSEntities = new HashMap<>();

		for(final SubGraphEntity subGraphEntity : subGraphEntities) {

			final int keyHash = subGraphEntity.getCSEntity().getKey().hashCode();
			final long csEntityOrderHash = Long.valueOf(subGraphEntity.getCSEntity().getEntityOrder()).hashCode();
			final int predicateHash = subGraphEntity.getPredicate().hashCode();
			Long subGraphHash = null;

			// TODO: calc sub graph hash

			long hash = keyHash;
			hash = 31 * hash + predicateHash;

			if(subGraphHash != null) {

				hash = 31 * hash + subGraphHash;
			}
			hash = 31 * hash + csEntityOrderHash;
			hash = 31 * hash +  Long.valueOf(subGraphEntity.getOrder()).hashCode();

			hashedCSEntities.put(Long.valueOf(hash).toString(), subGraphEntity);
		}

		return hashedCSEntities;
	}
}
