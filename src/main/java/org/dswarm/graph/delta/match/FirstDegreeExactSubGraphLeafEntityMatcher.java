package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dswarm.graph.delta.match.mark.SubGraphLeafEntityMarker;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphLeafEntityMatcher extends Matcher<SubGraphLeafEntity> {

	private static final Logger	LOG	= LoggerFactory.getLogger(FirstDegreeExactSubGraphLeafEntityMatcher.class);

	public FirstDegreeExactSubGraphLeafEntityMatcher(final Optional<? extends Collection<SubGraphLeafEntity>> existingSubGraphLeafEntitiesArg,
			final Optional<? extends Collection<SubGraphLeafEntity>> newSubGraphLeafEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String existingResourceURIArg, final String newResourceURIArg) {

		super(existingSubGraphLeafEntitiesArg, newSubGraphLeafEntitiesArg, existingResourceDBArg, newResourceDBArg, existingResourceURIArg,
				newResourceURIArg, new SubGraphLeafEntityMarker());
	}

	/**
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph leaf path hash + order
	 *
	 * @param subGraphLeafEntities
	 * @return
	 */
	@Override
	protected Map<String, SubGraphLeafEntity> generateHashes(final Collection<SubGraphLeafEntity> subGraphLeafEntities, final GraphDatabaseService graphDB) {

		final Map<String, SubGraphLeafEntity> hashedSubGraphLeafEntities = new HashMap<>();

		for(final SubGraphLeafEntity subGraphLeafEntity : subGraphLeafEntities) {

			final int keyHash = subGraphLeafEntity.getSubGraphEntity().getCSEntity().getKey().hashCode();
			final long csEntityOrderHash = Long.valueOf(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getEntityOrder()).hashCode();
			final int predicateHash = subGraphLeafEntity.getSubGraphEntity().getPredicate().hashCode();

			// calc sub graph leaf path hash
			final Long subGraphLeafPathHash = calculateSubGraphLeafPathHash(subGraphLeafEntity.getNodeId(),
					subGraphLeafEntity.getSubGraphEntity().getNodeId(), graphDB);

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

	private Long calculateSubGraphLeafPathHash(final Long leafNodeId, final Long entityNodeId, final GraphDatabaseService graphDB) {

		try (final Transaction ignored = graphDB.beginTx()) {

			final Iterable<Path> entityPaths =  GraphDBUtil.getEntityPaths(graphDB, entityNodeId, leafNodeId);
			if(entityPaths == null || !entityPaths.iterator().hasNext()) {

				return null;
			}

			final Path entityPath = entityPaths.iterator().next();
			final Iterator<Node> entityPathNodes = entityPath.reverseNodes().iterator();

			Long endNodeHash = GraphDBUtil.calculateNodeHash(entityPathNodes.next());

			if(endNodeHash == null) {

				return null;
			}

			Long subGraphLeafPathHash = (long) 0;

			final Iterable<Relationship> entityPathRels = entityPath.reverseRelationships();

			for(final Relationship entityPathRel : entityPathRels) {

				subGraphLeafPathHash = GraphDBUtil.calculateRelationshipHash(subGraphLeafPathHash, entityPathRel, endNodeHash);

				final Node endNode = entityPathNodes.next();
				endNodeHash = GraphDBUtil.calculateNodeHash(endNode);
			}

			return subGraphLeafPathHash;
		} catch (final Exception e) {

			FirstDegreeExactSubGraphLeafEntityMatcher.LOG.error("couldn't calculate sub graph leaf path hashes", e);
		}

		return null;
	}
}
