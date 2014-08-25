package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.mark.SubGraphEntityMarker;
import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphEntityMatcher extends Matcher<SubGraphEntity> {

	public FirstDegreeExactSubGraphEntityMatcher(final Collection<SubGraphEntity> existingSubGraphEntitiesArg,
			final Collection<SubGraphEntity> newSubGraphEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String existingResourceURIArg, final String newResourceURIArg) {

		super(existingSubGraphEntitiesArg, newSubGraphEntitiesArg, existingResourceDBArg, newResourceDBArg, existingResourceURIArg,
				newResourceURIArg, new SubGraphEntityMarker());
	}

	/**
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph hash + order
	 *
	 * @param subGraphEntities
	 * @return
	 */
	@Override
	protected Map<String, SubGraphEntity> generateHashes(final Collection<SubGraphEntity> subGraphEntities, final GraphDatabaseService graphDB) {

		final Map<String, SubGraphEntity> hashedSubGraphEntities = new HashMap<>();

		for(final SubGraphEntity subGraphEntity : subGraphEntities) {

			final int keyHash = subGraphEntity.getCSEntity().getKey().hashCode();
			final long csEntityOrderHash = Long.valueOf(subGraphEntity.getCSEntity().getEntityOrder()).hashCode();
			final int predicateHash = subGraphEntity.getPredicate().hashCode();

			// calc sub graph hash
			final Map<Long, Long> nodeHashes = new HashMap<>();
			final Integer deepestLeafHierarchyLevel = GraphDBUtil.calculateEntityLeafHashes(graphDB, subGraphEntity.getNodeId(), nodeHashes);

			if(deepestLeafHierarchyLevel != null && deepestLeafHierarchyLevel > subGraphEntity.getHierarchyLevel()) {

				int currentHierarchyLevel = deepestLeafHierarchyLevel - 1;

				while(currentHierarchyLevel > subGraphEntity.getHierarchyLevel()) {

					GraphDBUtil.calculateEntityHierarchyLevelNodesHashes(graphDB, subGraphEntity.getNodeId(), nodeHashes, currentHierarchyLevel);

					currentHierarchyLevel--;
				}

				GraphDBUtil.calculateSubGraphEntityHash(graphDB, subGraphEntity.getNodeId(), nodeHashes);
			}

			Long subGraphHash = nodeHashes.get(subGraphEntity.getNodeId());

			long hash = keyHash;
			hash = 31 * hash + predicateHash;

			if(subGraphHash != null) {

				hash = 31 * hash + subGraphHash;
			}
			hash = 31 * hash + csEntityOrderHash;
			hash = 31 * hash +  Long.valueOf(subGraphEntity.getOrder()).hashCode();

			hashedSubGraphEntities.put(Long.valueOf(hash).toString(), subGraphEntity);
		}

		return hashedSubGraphEntities;
	}
}
