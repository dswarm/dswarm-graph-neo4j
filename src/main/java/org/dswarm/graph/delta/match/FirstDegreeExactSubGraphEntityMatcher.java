package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphEntityMatcher extends SubGraphEntityMatcher {

	private final GraphDatabaseService existingResourceDB;
	private final GraphDatabaseService newResourceDB;

	public FirstDegreeExactSubGraphEntityMatcher(final Collection<SubGraphEntity> existingSubGraphEntitiesArg,
			final Collection<SubGraphEntity> newSubGraphEntitiesArg, final GraphDatabaseService existingResourceDBArg, final GraphDatabaseService newResourceDBArg) {

		super(existingSubGraphEntitiesArg, newSubGraphEntitiesArg);

		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;

		existingEntities = generateHashes(existingSubGraphEntitiesArg, existingResourceDB);
		newEntities = generateHashes(newSubGraphEntitiesArg, newResourceDB);
	}

	/**
	 * note: we need another method signature for calculating the hashes
	 *
	 * @param subGraphEntities
	 * @return
	 */
	@Override
	protected Map<String, SubGraphEntity> generateHashes(final Collection<SubGraphEntity> subGraphEntities) {

		return null;
	}

	/**
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph hash + order
	 *
	 * @param subGraphEntities
	 * @return
	 */
	protected Map<String, SubGraphEntity> generateHashes(final Collection<SubGraphEntity> subGraphEntities, final GraphDatabaseService graphDB) {

		final Map<String, SubGraphEntity> hashedCSEntities = new HashMap<>();

		for(final SubGraphEntity subGraphEntity : subGraphEntities) {

			final int keyHash = subGraphEntity.getCSEntity().getKey().hashCode();
			final long csEntityOrderHash = Long.valueOf(subGraphEntity.getCSEntity().getEntityOrder()).hashCode();
			final int predicateHash = subGraphEntity.getPredicate().hashCode();
			Long subGraphHash = null;

			// calc sub graph hash
			final Map<Long, Long> nodeHashes = new HashMap<>();
			final Integer deepestLeafHierarchyLevel = GraphDBUtil.calculateEntityLeafHashes(graphDB, subGraphEntity.getNodeId(), nodeHashes);

			// TODO: continue here

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
