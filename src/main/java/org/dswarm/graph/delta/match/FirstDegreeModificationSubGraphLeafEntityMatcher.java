package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.dswarm.graph.delta.match.model.GDMValueEntity;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public class FirstDegreeModificationSubGraphLeafEntityMatcher extends SubGraphLeafEntityMatcher implements ModificationResultSet<SubGraphLeafEntity> {

	private final GraphDatabaseService	existingResourceDB;
	private final GraphDatabaseService	newResourceDB;

	public FirstDegreeModificationSubGraphLeafEntityMatcher(final Collection<SubGraphLeafEntity> existingSubGraphLeafEntitiesArg,
			final Collection<SubGraphLeafEntity> newSubGraphLeafEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg) {

		super(existingSubGraphLeafEntitiesArg, newSubGraphLeafEntitiesArg);

		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;

		existingEntities = generateHashes(existingSubGraphLeafEntitiesArg, existingResourceDB);
		newEntities = generateHashes(newSubGraphLeafEntitiesArg, newResourceDB);
	}

	/**
	 * note: we need another method signature for calculating the hashes
	 *
	 * @param subGraphLeafEntities
	 * @return
	 */
	@Override
	protected Map<String, SubGraphLeafEntity> generateHashes(final Collection<SubGraphLeafEntity> subGraphLeafEntities) {

		return null;
	}

	/**
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph leaf path hash (leaf node without value) + order
	 *
	 * @param subGraphLeafEntities
	 * @return
	 */
	protected Map<String, SubGraphLeafEntity> generateHashes(Collection<SubGraphLeafEntity> subGraphLeafEntities, final GraphDatabaseService graphDB) {

		final Map<String, SubGraphLeafEntity> hashedSubGraphLeafEntities = new HashMap<>();

		for(final SubGraphLeafEntity subGraphLeafEntity : subGraphLeafEntities) {

			final int keyHash = subGraphLeafEntity.getSubGraphEntity().getCSEntity().getKey().hashCode();
			final long csEntityOrderHash = Long.valueOf(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getEntityOrder()).hashCode();
			final int predicateHash = subGraphLeafEntity.getSubGraphEntity().getPredicate().hashCode();

			// calc sub graph leaf path hash (leaf node without value)
			final Long subGraphLeafPathHash = GraphDBUtil
					.calculateSubGraphLeafPathModificationHash(subGraphLeafEntity.getNodeId(), subGraphLeafEntity.getSubGraphEntity().getNodeId(),
							graphDB);

			long hash = keyHash;
			hash = 31 * hash + predicateHash;

			if(subGraphLeafPathHash != null) {

				hash = 31 * hash + subGraphLeafPathHash;
			}

			hash = 31 * hash + csEntityOrderHash;
			hash = 31 * hash +  Long.valueOf(subGraphLeafEntity.getSubGraphEntity().getOrder()).hashCode();

			hashedSubGraphLeafEntities.put(Long.valueOf(hash).toString(), subGraphLeafEntity);
		}

		return hashedSubGraphLeafEntities;
	}

	@Override 
	public Map<SubGraphLeafEntity, SubGraphLeafEntity> getModifications() {

		final Map<SubGraphLeafEntity, SubGraphLeafEntity> modifications = new HashMap<>();
		matches = new HashSet<>();

		for (final Map.Entry<String, SubGraphLeafEntity> existingValueEntityEntry : existingEntities.entrySet()) {

			if (newEntities.containsKey(existingValueEntityEntry.getKey())) {

				final SubGraphLeafEntity existingValueEntity = existingValueEntityEntry.getValue();
				final SubGraphLeafEntity newValueEntity = newEntities.get(existingValueEntityEntry.getKey());

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
