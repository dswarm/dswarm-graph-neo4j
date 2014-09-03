package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.mark.SubGraphEntityMarker;
import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphEntityMatcher extends Matcher<SubGraphEntity> {

	private static final Logger	LOG	= LoggerFactory.getLogger(FirstDegreeExactSubGraphEntityMatcher.class);

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
			final Integer deepestLeafHierarchyLevel = calculateEntityLeafHashes(graphDB, subGraphEntity.getNodeId(), nodeHashes);

			if(deepestLeafHierarchyLevel != null && deepestLeafHierarchyLevel > subGraphEntity.getHierarchyLevel()) {

				int currentHierarchyLevel = deepestLeafHierarchyLevel - 1;

				while(currentHierarchyLevel > subGraphEntity.getHierarchyLevel()) {

					calculateEntityHierarchyLevelNodesHashes(graphDB, subGraphEntity.getNodeId(), nodeHashes, currentHierarchyLevel);

					currentHierarchyLevel--;
				}

				calculateSubGraphEntityHash(graphDB, subGraphEntity.getNodeId(), nodeHashes);
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

	private void calculateSubGraphEntityHash(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes) {

		final Transaction tx = graphDB.beginTx();

		try {

			GraphDBUtil.calculateEntityHash(graphDB, entityNodeId, nodeHashes);

			tx.success();
		} catch (final Exception e) {

			FirstDegreeExactSubGraphEntityMatcher.LOG.error("couldn't calculate sub graph entity hashes", e);

			tx.failure();
		} finally {

			tx.close();
		}
	}

	private void calculateEntityHierarchyLevelNodesHashes(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes, final int hierarchyLevel) {

		final Collection<String> entityHierarchyLevelNodeIds = getEntityHierarchyLevelNodes(graphDB, entityNodeId, hierarchyLevel);

		try (final Transaction ignored = graphDB.beginTx()) {

			for(final String entityHierarchyLevelNodeId : entityHierarchyLevelNodeIds) {

				GraphDBUtil.calculateEntityHash(graphDB, Long.valueOf(entityHierarchyLevelNodeId), nodeHashes);
			}
		} catch (final Exception e) {

			FirstDegreeExactSubGraphEntityMatcher.LOG.error("couldn't calculate simple node hashes", e);
		}
	}

	private Integer calculateEntityLeafHashes(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes) {

		final Collection<String> entityLeafNodeIds = GraphDBUtil.getEntityLeafs(graphDB, entityNodeId);
		Integer deepestHierarchyLevel = null;

		try (final Transaction ignored = graphDB.beginTx()) {

			for(final String entityLeafNodeId : entityLeafNodeIds) {

				final Node leafNode = graphDB.getNodeById(Long.valueOf(entityLeafNodeId));


				Long hash = GraphDBUtil.calculateNodeHash(leafNode);

				if(hash == null) {

					continue;
				}

				final Integer hierarchyLevel = (Integer) leafNode.getProperty("__HIERARCHY_LEVEL__", null);

				if(hierarchyLevel != null) {

					hash = 31 * hash + hierarchyLevel;

					if(deepestHierarchyLevel != null && deepestHierarchyLevel < hierarchyLevel) {

						deepestHierarchyLevel = hierarchyLevel;
					} else {

						deepestHierarchyLevel = hierarchyLevel;
					}
				}

				nodeHashes.put(leafNode.getId(), hash);
			}
		} catch (final Exception e) {

			FirstDegreeExactSubGraphEntityMatcher.LOG.error("couldn't calculated entity leaf hashes", e);
		}

		return deepestHierarchyLevel;
	}

	/**
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	private Collection<String> getEntityHierarchyLevelNodes(final GraphDatabaseService graphDB, final long nodeId, final int hierarchyLevel) {

		final String entitHierarchyLevelNodesQuery = buildGetEntityHierarchyLevelNodesQuery(nodeId, hierarchyLevel);

		return GraphDBUtil.executeQueryWithMultipleResults(entitHierarchyLevelNodesQuery, "hierarchy_level_node", graphDB);
	}

	private String buildGetEntityHierarchyLevelNodesQuery(final long nodeId, final int hierarchyLevel) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m)").append("\nWHERE m.__HIERARCHY_LEVEL__ = ").append(hierarchyLevel)
				.append("\nRETURN id(m) AS hierarchy_level_node");

		return sb.toString();
	}
}
