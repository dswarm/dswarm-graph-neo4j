package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphLeafEntityMatcher extends SubGraphLeafEntityMatcher {

	private final GraphDatabaseService	existingResourceDB;
	private final GraphDatabaseService	newResourceDB;

	public FirstDegreeExactSubGraphLeafEntityMatcher(final Collection<SubGraphLeafEntity> existingSubGraphLeafEntitiesArg,
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
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph leaf path hash + order
	 *
	 * @param subGraphLeafEntities
	 * @return
	 */
	protected Map<String, SubGraphLeafEntity> generateHashes(final Collection<SubGraphLeafEntity> subGraphLeafEntities, final GraphDatabaseService graphDB) {

		final Map<String, SubGraphLeafEntity> hashedSubGraphLeafEntities = new HashMap<>();

		for(final SubGraphLeafEntity subGraphLeafEntity : subGraphLeafEntities) {

			final int keyHash = subGraphLeafEntity.getSubGraphEntity().getCSEntity().getKey().hashCode();
			final long csEntityOrderHash = Long.valueOf(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getEntityOrder()).hashCode();
			final int predicateHash = subGraphLeafEntity.getSubGraphEntity().getPredicate().hashCode();

			// calc sub graph leaf path hash
			final Long subGraphLeafPathHash = GraphDBUtil.calculateSubGraphLeafPathHash(subGraphLeafEntity.getNodeId(), subGraphLeafEntity.getSubGraphEntity().getNodeId(), graphDB);

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
}
