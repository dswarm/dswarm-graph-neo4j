/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.match.mark.SubGraphLeafEntityMarker;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class FirstDegreeExactSubGraphLeafEntityMatcher extends Matcher<SubGraphLeafEntity> {

	private static final Logger	LOG	= LoggerFactory.getLogger(FirstDegreeExactSubGraphLeafEntityMatcher.class);

	public FirstDegreeExactSubGraphLeafEntityMatcher(final Optional<? extends Collection<SubGraphLeafEntity>> existingSubGraphLeafEntitiesArg,
			final Optional<? extends Collection<SubGraphLeafEntity>> newSubGraphLeafEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String prefixedExistingResourceURIArg, final String prefixedNewResourceURIArg)
			throws DMPGraphException {

		super(existingSubGraphLeafEntitiesArg, newSubGraphLeafEntitiesArg, existingResourceDBArg, newResourceDBArg, prefixedExistingResourceURIArg,
				prefixedNewResourceURIArg, new SubGraphLeafEntityMarker());
	}

	/**
	 * hash with key (from cs entity) + cs entity order + predicate + sub graph leaf path hash + order
	 *
	 * @param subGraphLeafEntities
	 * @return
	 */
	@Override
	protected Map<String, SubGraphLeafEntity> generateHashes(final Collection<SubGraphLeafEntity> subGraphLeafEntities, final GraphDatabaseService graphDB) throws DMPGraphException {

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

	private Long calculateSubGraphLeafPathHash(final Long leafNodeId, final Long entityNodeId, final GraphDatabaseService graphDB)
			throws DMPGraphException {

		try(final Transaction tx = graphDB.beginTx()) {

			final Iterable<Path> entityPaths = GraphDBUtil.getEntityPaths(graphDB, entityNodeId, leafNodeId);
			if (entityPaths == null || !entityPaths.iterator().hasNext()) {

				tx.success();

				return null;
			}

			final Path entityPath = entityPaths.iterator().next();
			final Iterator<Node> entityPathNodes = entityPath.reverseNodes().iterator();

			Long endNodeHash = GraphDBUtil.calculateNodeHash(entityPathNodes.next());

			if (endNodeHash == null) {

				tx.success();

				return null;
			}

			Long subGraphLeafPathHash = (long) 0;

			final Iterable<Relationship> entityPathRels = entityPath.reverseRelationships();

			for (final Relationship entityPathRel : entityPathRels) {

				subGraphLeafPathHash = GraphDBUtil.calculateRelationshipHash(subGraphLeafPathHash, entityPathRel, endNodeHash);

				final Node endNode = entityPathNodes.next();
				endNodeHash = GraphDBUtil.calculateNodeHash(endNode);
			}

			final Long result = subGraphLeafPathHash;

			tx.success();

			return result;
		} catch (final Exception e) {

			final String message = "couldn't calculate sub graph leaf path hashes";

			FirstDegreeExactSubGraphLeafEntityMatcher.LOG.error(message, e);
			
			throw new DMPGraphException(message);
		}
	}
}
