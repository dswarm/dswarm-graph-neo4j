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
package org.dswarm.graph.delta.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.lambdaj.Lambda;
import ch.lambdaj.group.Group;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.model.Attribute;
import org.dswarm.common.model.AttributePath;
import org.dswarm.common.model.ContentSchema;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.delta.evaluator.EntityEvaluator;
import org.dswarm.graph.delta.evaluator.StatementEvaluator;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.GDMValueEntity;
import org.dswarm.graph.delta.match.model.KeyEntity;
import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public final class GraphDBUtil {

	private static final Logger LOG = LoggerFactory.getLogger(GraphDBUtil.class);

	private static final RelationshipType rdfTypeRelType = DynamicRelationshipType.withName("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

	public static void addNodeId(final Set<Long> nodeIds, final Long nodeId) throws DMPGraphException {

		if (nodeId == null) {

			// TODO: remove this, if it bugs you ;)
			GraphDBUtil.LOG.debug("node id was null");

			return;
		}

		if (nodeId == -1) {

			final String message = "node id shouldn't be '-1'";

			GraphDBUtil.LOG.error(message);

			throw new DMPGraphException(message);
		}

		nodeIds.add(nodeId);
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param resourceURI
	 * @return
	 */
	public static Node getResourceNode(final GraphDatabaseService graphDB, final String resourceURI) {

		final Index<Node> resources = graphDB.index().forNodes(GraphIndexStatics.RESOURCES_INDEX_NAME);

		if (resources == null) {

			return null;
		}

		final IndexHits<Node> hits = resources.get(GraphStatics.URI, resourceURI);

		if (hits == null || !hits.hasNext()) {

			if (hits != null) {

				hits.close();
			}

			return null;
		}

		final Node node = hits.next();

		hits.close();

		return node;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param resourceURI
	 * @return
	 */
	public static Node getResourceNode(final GraphDatabaseService graphDB, final String resourceURI, final String dataModelURI) {

		final long resourceUriDataModelUriHash = HashUtils.generateHash(resourceURI + dataModelURI);

		return graphDB.findNode(BasicNeo4jProcessor.RESOURCE_LABEL, GraphStatics.HASH, resourceUriDataModelUriHash);
	}

	static String getLabels(final Node node) {

		final StringBuilder sb2 = new StringBuilder();

		for (final Label label : node.getLabels()) {

			sb2.append(label.name()).append(",");
		}

		final String tempLabels = sb2.toString();

		return tempLabels.substring(0, tempLabels.length() - 1);
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param resourceURI
	 * @return
	 */
	public static Iterable<Path> getResourcePaths(final GraphDatabaseService graphDB, final String resourceURI) {

		final Node resourceNode = getResourceNode(graphDB, resourceURI);

		// TODO: maybe replace with gethEntityPaths(GraphdataBaseService, Node)
		final Iterable<Path> paths = graphDB.traversalDescription().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
				.order(BranchOrderingPolicies.POSTORDER_BREADTH_FIRST).expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build())
				.evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						final boolean hasLeafLabel = path.endNode().hasLabel(GraphProcessingStatics.LEAF_LABEL);

						if (hasLeafLabel) {

							return Evaluation.INCLUDE_AND_CONTINUE;
						}
						return Evaluation.EXCLUDE_AND_CONTINUE;
					}
				}).traverse(resourceNode);

		return paths;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param resourceURI
	 * @param dataModelURI
	 * @return
	 */
	public static Iterable<Path> getResourcePaths(final GraphDatabaseService graphDB, final String resourceURI, final String dataModelURI) {

		final Node resourceNode = getResourceNode(graphDB, resourceURI, dataModelURI);

		return getResourcePaths(graphDB, resourceNode);
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param resourceNode
	 * @return
	 */
	public static Iterable<Path> getResourcePaths(final GraphDatabaseService graphDB, final Node resourceNode) {

		// TODO: maybe replace with gethEntityPaths(GraphdataBaseService, Node)
		final Iterable<Path> paths = graphDB.traversalDescription().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
				.order(BranchOrderingPolicies.POSTORDER_BREADTH_FIRST).expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build())
				.evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						final boolean reachedEndOfResourcePath =
								path.length() >= 1 && (path.endNode().hasProperty(GraphStatics.URI_PROPERTY) || path.endNode()
										.hasProperty(GraphStatics.VALUE_PROPERTY));

						if (reachedEndOfResourcePath) {

							return Evaluation.INCLUDE_AND_CONTINUE;
						}

						return Evaluation.EXCLUDE_AND_CONTINUE;
					}
				}).traverse(resourceNode);

		return paths;
	}

	/**
	 * @param graphDB
	 * @return
	 */
	public static boolean checkGraphMatchingCompleteness(final GraphDatabaseService graphDB) throws DMPGraphException {

		try (final Transaction tx = graphDB.beginTx()) {

			final Iterable<Relationship> rels = GlobalGraphOperations.at(graphDB).getAllRelationships();

			boolean incomplete = false;

			for (final Relationship rel : rels) {

				final Boolean relMatchedState = (Boolean) rel.getProperty(DeltaStatics.MATCHED_PROPERTY, null);
				final boolean finalRelMatchedState = checkMatchedState(relMatchedState, rel.getId(), DeltaStatics.RELATIONSHIP_TYPE);

				if (!finalRelMatchedState) {

					incomplete = true;

					break;
				}

				final Boolean subjectMatchedState = (Boolean) rel.getStartNode().getProperty(DeltaStatics.MATCHED_PROPERTY, null);
				final boolean finalSubjectMatchedState = checkMatchedState(subjectMatchedState, rel.getStartNode().getId(), DeltaStatics.NODE_TYPE);

				if (!finalSubjectMatchedState) {

					incomplete = true;

					break;
				}

				final Boolean objectMatchedState = (Boolean) rel.getEndNode().getProperty(DeltaStatics.MATCHED_PROPERTY, null);
				final boolean finalObjectMatchedState = checkMatchedState(objectMatchedState, rel.getEndNode().getId(), DeltaStatics.NODE_TYPE);

				if (!finalObjectMatchedState) {

					incomplete = true;

					break;
				}
			}

			final boolean result = !incomplete;

			tx.success();

			return result;
		} catch (final Exception e) {

			final String message = "couldn't complete the graph matching completeness check for graph DB '" + graphDB.toString() + "'";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	private static boolean checkMatchedState(final Boolean matchedState, final long id, final String type) {

		if (matchedState == null) {

			GraphDBUtil.LOG.error(type + " '" + id + "' couldn't be matched, i.e., there was no match state available");

			return false;
		}

		if (!matchedState) {

			GraphDBUtil.LOG.error(type + " '" + id + "' couldn't be matched, i.e., there was no match state was 'false'");

			return false;
		}

		return matchedState;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	public static Iterable<Path> getEntityPaths(final GraphDatabaseService graphDB, final long nodeId) {

		final Node entityNode = graphDB.getNodeById(nodeId);

		return getEntityPaths(graphDB, entityNode);
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param entityNode
	 * @return
	 */
	private static Iterable<Path> getEntityPaths(final GraphDatabaseService graphDB, final Node entityNode) {

		return graphDB.traversalDescription().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL).order(BranchOrderingPolicies.POSTORDER_BREADTH_FIRST)
				.expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build()).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						final boolean hasLeafLabel = path.endNode().hasLabel(GraphProcessingStatics.LEAF_LABEL);

						if (hasLeafLabel) {

							return Evaluation.INCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_CONTINUE;
					}
				}).traverse(entityNode);
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param entityId
	 * @param leafNodeId
	 * @return
	 */
	public static Iterable<Path> getEntityPaths(final GraphDatabaseService graphDB, final long entityId, final long leafNodeId) {

		final Node entityNode = graphDB.getNodeById(entityId);

		return graphDB.traversalDescription().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL).order(BranchOrderingPolicies.POSTORDER_BREADTH_FIRST)
				.expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build()).evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						final boolean hasLeafLabel = path.endNode().hasLabel(GraphProcessingStatics.LEAF_LABEL);

						if (hasLeafLabel && path.endNode().getId() == leafNodeId) {

							return Evaluation.INCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_CONTINUE;
					}
				}).traverse(entityNode);
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param entityNodeId
	 * @param nodeHashes
	 * @return
	 */
	public static boolean calculateEntityHash(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes) {

		final Node entityNode = graphDB.getNodeById(entityNodeId);

		Long hash = calculateNodeHash(entityNode);

		if (hash == null) {

			return false;
		}

		// add hashed from child nodes
		for (final Relationship rel : entityNode.getRelationships(Direction.OUTGOING)) {

			final Node endNode = rel.getEndNode();
			final Long endNodeHash = nodeHashes.get(endNode.getId());
			hash = calculateRelationshipHash(hash, rel, endNodeHash);
		}

		nodeHashes.put(entityNode.getId(), hash);

		return true;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param hash
	 * @param rel
	 * @param endNodeHash
	 * @return
	 */
	public static long calculateRelationshipHash(long hash, final Relationship rel, final Long endNodeHash) {

		final String predicate = rel.getType().name();
		final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

		long childNodeHash = predicate.hashCode();

		if (order != null) {

			childNodeHash = 31 * childNodeHash + order.hashCode();
		}

		if (endNodeHash != null) {

			childNodeHash = 31 * childNodeHash + endNodeHash;
		}

		hash = 31 * hash + childNodeHash;
		return hash;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param node
	 * @return
	 */
	public static Long calculateNodeHash(final Node node) {

		final NodeType nodeType = getNodeType(node);

		if (nodeType == null) {

			// // skip none typed nodes?
			return null;
		}

		final String value = getValue(node, nodeType);

		long leafNodeHash = nodeType.hashCode();

		if (value != null) {

			leafNodeHash = 31 * leafNodeHash + value.hashCode();
		}

		return leafNodeHash;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param node
	 * @return
	 */
	public static Long calculateSimpleNodeHash(final Node node) {

		final NodeType nodeType = getNodeType(node);

		if (nodeType == null) {

			// // skip none typed nodes?
			return null;
		}

		return (long) nodeType.hashCode();
	}

	private static NodeType getNodeType(final Node node) {

		final String nodeTypeString = (String) node.getProperty(GraphStatics.NODETYPE_PROPERTY, null);

		if (nodeTypeString == null) {

			return null;
		}

		return NodeType.getByName(nodeTypeString);
	}

	private static String getValue(final Node node, final NodeType nodeType) {

		final String value;

		switch (nodeType) {

			case Resource:
			case TypeResource:

				String tempValue = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);

				final String dataModel = (String) node.getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

				if (dataModel != null) {

					tempValue += dataModel;
				}

				value = tempValue;

				break;
			case Literal:

				value = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);

				break;
			default:

				value = null;
		}

		return value;
	}

	/**
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	public static Collection<String> getEntityLeafs(final GraphDatabaseService graphDB, final long nodeId) throws DMPGraphException {

		final String entityLeafsQuery = buildGetEntityLeafsQuery(nodeId);

		return executeQueryWithMultipleResults(entityLeafsQuery, "leaf_node", graphDB);
	}

	/**
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	private static Map<String, String> getEntityLeafsWithValue(final GraphDatabaseService graphDB, final long nodeId) throws DMPGraphException {

		final String entityLeafsQuery = buildGetEntityLeafsWithValueQuery(nodeId);

		return executeQueryWithMultipleResultsWithValues(entityLeafsQuery, "leaf_node", "leaf_uri", "leaf_value", graphDB);
	}

	/**
	 * note: should be executed in transaction scope
	 *
	 * @param graphDB
	 * @param pathEndNodeIds
	 * @param nodeId
	 */
	public static void fetchEntityTypeNodes(final GraphDatabaseService graphDB, final Set<Long> pathEndNodeIds, final long nodeId)
			throws DMPGraphException {

		// fetch type nodes as well
		final Iterable<Relationship> typeRels = graphDB.getNodeById(nodeId).getRelationships(Direction.OUTGOING, rdfTypeRelType);

		if (typeRels != null && typeRels.iterator().hasNext()) {

			for (final Relationship typeRel : typeRels) {

				// TODO: could be removed later
				GraphDBUtil.LOG.debug("fetch entity type rel: '" + typeRel.getId() + "'");

				GraphDBUtil.addNodeId(pathEndNodeIds, typeRel.getEndNode().getId());
			}
		}
	}

	/**
	 * note: should be executed in transaction scope
	 *
	 * @param deltaState
	 * @param graphDB
	 * @param pathEndNodeIds
	 * @param nodeId
	 */
	public static void determineNonMatchedSubGraphPathEndNodes(final DeltaState deltaState, final GraphDatabaseService graphDB,
			final Set<Long> pathEndNodeIds, final long nodeId) throws DMPGraphException {

		if (deltaState.equals(DeltaState.ADDITION) || deltaState.equals(DeltaState.DELETION)) {

			final Iterable<Path> nonMatchedSubGraphPaths = getNonMatchedSubGraphPaths(nodeId, graphDB);

			if (nonMatchedSubGraphPaths != null && nonMatchedSubGraphPaths.iterator().hasNext()) {

				for (final Path nonMatchtedSubGraphPath : nonMatchedSubGraphPaths) {

					GraphDBUtil.addNodeId(pathEndNodeIds, nonMatchtedSubGraphPath.endNode().getId());
				}
			}
		}
	}

	public static Collection<CSEntity> getCSEntities(final GraphDatabaseService graphDB, final String resourceURI,
			final AttributePath commonAttributePath, final ContentSchema contentSchema)
			throws DMPGraphException {

		final Map<Long, CSEntity> csEntities = new LinkedHashMap<>();

		try (final Transaction tx = graphDB.beginTx()) {

			final Node resourceNode = getResourceNode(graphDB, resourceURI);

			// determine CS entity nodes
			final ResourceIterable<Node> csEntityNodes = graphDB.traversalDescription().breadthFirst()
					.evaluator(Evaluators.toDepth(commonAttributePath.getAttributes().size()))
					.evaluator(new EntityEvaluator(commonAttributePath.getAttributes()))
					.traverse(resourceNode).nodes();

			if (csEntityNodes == null) {

				tx.success();

				return null;
			}

			for (final Node node : csEntityNodes) {

				final CSEntity csEntity = new CSEntity(node.getId());
				csEntities.put(csEntity.getNodeId(), csEntity);
			}

			final ArrayList<Node> csEntityNodesList = Lists.newArrayList(csEntityNodes);
			final Node[] csEntityNodesArray = new Node[csEntityNodesList.size()];
			csEntityNodesList.toArray(csEntityNodesArray);

			if (contentSchema.getKeyAttributePaths() != null) {

				// determine key entities
				determineKeyEntities(graphDB, commonAttributePath, contentSchema, csEntities, csEntityNodesArray);
			}

			if (contentSchema.getValueAttributePath() != null) {

				// determine value entities
				determineValueEntities(graphDB, commonAttributePath, contentSchema, csEntities, csEntityNodesArray);
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't determine cs entities successfully";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		final Collection<CSEntity> csEntitiesCollection = csEntities.values();

		// determine cs entity order
		determineCSEntityOrder(csEntitiesCollection);

		return csEntitiesCollection;
	}

	public static Optional<? extends Collection<SubGraphLeafEntity>> getSubGraphLeafEntities(
			final Optional<? extends Collection<SubGraphEntity>> subGraphEntities, final GraphDatabaseService graphDB)
			throws DMPGraphException {

		if (!subGraphEntities.isPresent()) {

			return Optional.absent();
		}

		final Set<SubGraphLeafEntity> subGraphLeafEntities = new HashSet<>();

		for (final SubGraphEntity subGraphEntity : subGraphEntities.get()) {

			final Map<String, String> subGraphLeafs = GraphDBUtil.getEntityLeafsWithValue(graphDB, subGraphEntity.getNodeId());

			if (subGraphLeafs == null || subGraphLeafs.isEmpty()) {

				continue;
			}

			for (final Map.Entry<String, String> subGraphLeafEntry : subGraphLeafs.entrySet()) {

				final SubGraphLeafEntity subGraphLeafEntity = new SubGraphLeafEntity(Long.valueOf(subGraphLeafEntry.getKey()),
						subGraphLeafEntry.getValue(), subGraphEntity);

				subGraphLeafEntities.add(subGraphLeafEntity);
			}
		}

		return Optional.of(subGraphLeafEntities);
	}

	/**
	 * note: should be executed in transaction scope
	 *
	 * @param nodeId
	 * @param graphDB
	 * @return
	 */
	public static Iterable<Path> getNonMatchedSubGraphPaths(final long nodeId, final GraphDatabaseService graphDB) {

		final Node entityNode = graphDB.getNodeById(nodeId);

		// final int entityNodeHierarchyLevel = (int) entityNode.getProperty("__HIERARCHY_LEVEL__");

		final Iterable<Path> paths = graphDB.traversalDescription().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
				.order(BranchOrderingPolicies.POSTORDER_BREADTH_FIRST).expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build())
				.evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						// if (entityNodeHierarchyLevel > (int) path.endNode().getProperty("__HIERARCHY_LEVEL__")) {
						//
						// return Evaluation.EXCLUDE_AND_PRUNE;
						// }

						if (path.lastRelationship() == null && path.endNode().hasLabel(GraphProcessingStatics.LEAF_LABEL)) {

							return Evaluation.EXCLUDE_AND_PRUNE;
						}

						if (path.lastRelationship() == null) {

							return Evaluation.EXCLUDE_AND_CONTINUE;
						}

						if (path.lastRelationship().hasProperty(DeltaStatics.MATCHED_PROPERTY)) {

							// include only non-matched relationships (paths)
							return Evaluation.EXCLUDE_AND_PRUNE;
						}

						final boolean hasLeafLabel = path.endNode().hasLabel(GraphProcessingStatics.LEAF_LABEL);

						if (hasLeafLabel) {

							return Evaluation.INCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_CONTINUE;
					}
				}).traverse(entityNode);

		return paths;
	}

	public static Collection<SubGraphEntity> determineNonMatchedCSEntitySubGraphs(final Collection<CSEntity> csEntities,
			final GraphDatabaseService graphDB)
			throws DMPGraphException {

		final Set<SubGraphEntity> subgraphEntities = new HashSet<>();

		try (final Transaction tx = graphDB.beginTx()) {

			for (final CSEntity csEntity : csEntities) {

				final Node csEntityNode = graphDB.getNodeById(csEntity.getNodeId());
				final Iterable<Relationship> csEntityOutgoingRels = csEntityNode.getRelationships(Direction.OUTGOING);

				if (csEntityOutgoingRels == null || !csEntityOutgoingRels.iterator().hasNext()) {

					continue;
				}

				final List<Relationship> nonMatchedRels = new ArrayList<>();

				for (final Relationship csEntityOutgoingRel : csEntityOutgoingRels) {

					if (csEntityOutgoingRel.hasProperty(DeltaStatics.MATCHED_PROPERTY)) {

						continue;
					}

					nonMatchedRels.add(csEntityOutgoingRel);
				}

				if (nonMatchedRels.isEmpty()) {

					// everything is already matched
					continue;
				}

				final String deltaStateString = (String) csEntityNode.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);
				final DeltaState deltaState;

				if (deltaStateString != null) {

					deltaState = DeltaState.getByName(deltaStateString);
				} else {

					deltaState = null;
				}

				for (final Relationship nonMatchedRel : nonMatchedRels) {

					final String predicate = nonMatchedRel.getType().name();
					final Long order = (Long) nonMatchedRel.getProperty(GraphStatics.ORDER_PROPERTY, null);
					final long finalOrder;

					if (order != null) {

						finalOrder = order;
					} else {

						finalOrder = 1;
					}

					// TODO: remove this later, it'S just for debugging purpose right now
					if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {

						final long relId = nonMatchedRel.getId();
						final String value = (String) nonMatchedRel.getEndNode().getProperty(GraphStatics.URI_PROPERTY, null);

						final String logout = "in determineNonMatchedCSEntitySubGraphs with rel id = '" + relId + "', value = '" + value + "'";
						System.out.println(logout);
						final String relPrint = GraphDBPrintUtil.printDeltaRelationship(nonMatchedRel);
						GraphDBUtil.LOG.debug(logout);
						GraphDBUtil.LOG.debug(relPrint);
					}

					final long endNodeId = nonMatchedRel.getEndNode().getId();
					final int endNodeHierarchyLevel = (int) nonMatchedRel.getEndNode().getProperty("__HIERARCHY_LEVEL__");

					final SubGraphEntity subGraphEntity = new SubGraphEntity(endNodeId, predicate, deltaState, csEntity, finalOrder,
							endNodeHierarchyLevel);
					subgraphEntities.add(subGraphEntity);
				}
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't determine non-matched cs entity sub graphs";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		return subgraphEntities;
	}

	public static String determineRecordIdentifier(final GraphDatabaseService graphDB, final AttributePath recordIdentifierAP, final String recordURI)
			throws DMPGraphException {

		final String query = buildGetRecordIdentifierQuery(recordIdentifierAP, recordURI);

		return executeQueryWithSingleResult(query, "record_identifier", graphDB);
	}

	public static String determineRecordUri(final String recordId, final AttributePath recordIdentifierAP, final String dataModelUri,
			final GraphDatabaseService graphDB) throws DMPGraphException {

		final String query = buildGetRecordUriQuery(recordId, recordIdentifierAP, dataModelUri);

		return executeQueryWithSingleResult(query, "record_uri", graphDB);
	}

	public static Collection<String> determineRecordUris(final String searchValue, final AttributePath keyAttributePath, final String dataModelUri,
			final GraphDatabaseService graphDB) throws DMPGraphException {

		final String query = buildGetRecordUrisQuery(searchValue, keyAttributePath, dataModelUri);

		return executeQueryWithMultipleResults(query, "record_uri", graphDB);
	}

	public static Collection<ValueEntity> getFlatResourceNodeValues(final String resourceURI, final GraphDatabaseService graphDB)
			throws DMPGraphException {

		try (final Transaction tx = graphDB.beginTx()) {

			final Node resourceNode = getResourceNode(graphDB, resourceURI);

			final Collection<ValueEntity> flatResourceNodeValues = getFlatNodeValues(resourceNode, graphDB);

			tx.success();

			return flatResourceNodeValues;
		} catch (final Exception e) {

			final String message = "couldn't determine record uri";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	private static Collection<ValueEntity> getFlatNodeValues(final Node node, final GraphDatabaseService graphDB) {

		// determine flat values
		final Iterable<Path> paths = graphDB.traversalDescription().breadthFirst().evaluator(new StatementEvaluator(node.getId())).traverse(node);

		if (paths == null || !paths.iterator().hasNext()) {

			return null;
		}

		final Map<String, CSEntity> valuesMap = new HashMap<>();

		for (final Path path : paths) {

			final Relationship rel = path.lastRelationship();
			final String predicate = rel.getType().name();
			final long valueNodeId = path.endNode().getId();

			final String nodeTypeString = (String) path.endNode().getProperty(GraphStatics.NODETYPE_PROPERTY, null);

			if (nodeTypeString == null) {

				// skip none typed nodes?
				continue;
			}

			final NodeType valueNodeType = NodeType.getByName(nodeTypeString);
			final String value;

			switch (valueNodeType) {

				case Resource:
				case TypeResource:

					String tempValue = (String) path.endNode().getProperty(GraphStatics.URI_PROPERTY, null);

					final String dataModel = (String) path.endNode().getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

					if (dataModel != null) {

						tempValue += dataModel;
					}

					value = tempValue;

					break;
				case Literal:

					value = (String) path.endNode().getProperty(GraphStatics.VALUE_PROPERTY, null);

					break;
				default:

					value = null;
			}

			final Long valueOrder = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

			final long finalValueOrder;

			if (valueOrder != null) {

				finalValueOrder = valueOrder;
			} else {

				finalValueOrder = (long) 1;
			}

			final GDMValueEntity valueEntity = new GDMValueEntity(valueNodeId, value, finalValueOrder, valueNodeType);

			if (!valuesMap.containsKey(predicate)) {

				final CSEntity csEntity = new CSEntity();
				final KeyEntity keyEntity = new KeyEntity(predicate);
				csEntity.addKeyEntity(keyEntity);

				valuesMap.put(predicate, csEntity);
			}

			valuesMap.get(predicate).addValueEntity(valueEntity);
		}

		final List<ValueEntity> values = new ArrayList<>();

		for (final CSEntity csEntity : valuesMap.values()) {

			values.addAll(csEntity.getValueEntities());
		}

		return values;
	}

	private static void determineKeyEntities(final GraphDatabaseService graphDB, final AttributePath commonAttributePath,
			final ContentSchema contentSchema, final Map<Long, CSEntity> csEntities, final Node[] csEntityNodesArray) {

		for (final AttributePath keyAttributePath : contentSchema.getKeyAttributePaths()) {

			final LinkedList<Attribute> relativeKeyAttributePath = determineRelativeAttributePath(keyAttributePath, commonAttributePath);

			final Iterable<Path> relativeKeyPaths = graphDB.traversalDescription().depthFirst()
					.evaluator(Evaluators.toDepth(relativeKeyAttributePath.size())).evaluator(new EntityEvaluator(relativeKeyAttributePath))
					.traverse(csEntityNodesArray);

			for (final Path relativeKeyPath : relativeKeyPaths) {

				final Node keyNode = relativeKeyPath.endNode();
				final Node csEntityNode = relativeKeyPath.startNode();
				final String keyValue = (String) keyNode.getProperty(GraphStatics.VALUE_PROPERTY, null);
				final KeyEntity keyEntity = new KeyEntity(keyNode.getId(), keyValue);
				csEntities.get(csEntityNode.getId()).addKeyEntity(keyEntity);
			}
		}
	}

	private static void determineValueEntities(final GraphDatabaseService graphDB, final AttributePath commonAttributePath,
			final ContentSchema contentSchema, final Map<Long, CSEntity> csEntities, final Node[] csEntityNodesArray) {

		final LinkedList<Attribute> relativeValueAttributePath = determineRelativeAttributePath(contentSchema.getValueAttributePath(),
				commonAttributePath);

		final Iterable<Path> relativeValuePaths = graphDB.traversalDescription().depthFirst()
				.evaluator(Evaluators.toDepth(relativeValueAttributePath.size())).evaluator(new EntityEvaluator(relativeValueAttributePath))
				.traverse(csEntityNodesArray);

		for (final Path relativeValuePath : relativeValuePaths) {

			final Node valueNode = relativeValuePath.endNode();
			final Node csEntityNode = relativeValuePath.startNode();
			final String valueValue = (String) valueNode.getProperty(GraphStatics.VALUE_PROPERTY, null);
			final Long valueOrder = (Long) relativeValuePath.lastRelationship().getProperty(GraphStatics.ORDER_PROPERTY, null);
			final ValueEntity valueEntity = new ValueEntity(valueNode.getId(), valueValue, valueOrder);
			csEntities.get(csEntityNode.getId()).addValueEntity(valueEntity);
		}
	}

	private static LinkedList<Attribute> determineRelativeAttributePath(final AttributePath attributePath, final AttributePath commonAttributePath) {

		final Iterator<Attribute> apIter = attributePath.getAttributes().iterator();
		final Iterator<Attribute> commonAPIter = commonAttributePath.getAttributes().iterator();

		while (apIter.hasNext()) {

			if (!commonAPIter.hasNext()) {

				break;
			}

			final Attribute apAttribute = apIter.next();
			final Attribute commonAttribute = commonAPIter.next();

			if (!apAttribute.equals(commonAttribute)) {

				break;
			}
		}

		if (!apIter.hasNext()) {

			return null;
		}

		final LinkedList<Attribute> relativeAttributePath = new LinkedList<>();

		while (apIter.hasNext()) {

			relativeAttributePath.add(apIter.next());
		}

		return relativeAttributePath;
	}

	private static void determineCSEntityOrder(final Collection<CSEntity> csEntities) {

		final Group<CSEntity> keyGroup = Lambda.group(csEntities, Lambda.by(Lambda.on(CSEntity.class).getKey()));

		for (final Group<CSEntity> csEntityKeyGroup : keyGroup.subgroups()) {

			int i = 1;

			for (final CSEntity csEntity : csEntityKeyGroup.findAll()) {

				csEntity.setEntityOrder((long) i);
				i++;
			}
		}
	}

	private static String buildGetRecordIdentifierQuery(final AttributePath recordIdentifierAP, final String recordURI) {

		// START n=node:resources(__URI__="http://data.slub-dresden.de/datamodels/7/records/a1280f78-5f96-4fe6-b916-5e38e5d620d3")
		// MATCH (n)-[r:`http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#id`]->(o)
		// WHERE n.__NODETYPE__ = "__RESOURCE__" AND
		// o.__NODETYPE__ = "__LITERAL__"
		// RETURN o.__VALUE__ AS record_identifier;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node:").append(GraphIndexStatics.RESOURCES_INDEX_NAME).append("(").append(GraphStatics.URI).append(" =\"")
				.append(recordURI).append("\")\nMATCH (n)");

		int i = 1;
		for (final Attribute attribute : recordIdentifierAP.getAttributes()) {

			sb.append("-[:`").append(attribute.getUri()).append("`]->");

			if (i < recordIdentifierAP.getAttributes().size()) {

				sb.append("()");
			}

			i++;
		}

		sb.append("(o)\n").append("WHERE n.").append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Resource).append("\" AND\no.")
				.append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Literal).append("\"\nRETURN o.")
				.append(GraphStatics.VALUE_PROPERTY).append(" AS record_identifier");

		return sb.toString();
	}

	private static String buildGetRecordUriQuery(final String recordId, final AttributePath recordIdentifierAP, final String dataModelUri) {

		// START n=node:values(__VALUE__="a1280f78-5f96-4fe6-b916-5e38e5d620d3")
		// MATCH (n)-[r:`http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#id`]->(o)
		// WHERE n.__NODETYPE__ = "__RESOURCE__" AND
		// o.__NODETYPE__ = "__LITERAL__"
		// RETURN o.__URI__ AS record_uri;

		final StringBuilder sb = new StringBuilder();

		sb.append("START o=node:").append(GraphIndexStatics.VALUES_INDEX_NAME).append("(").append(GraphStatics.VALUE).append("=\"").append(recordId)
				.append("\")\nMATCH (n)");

		int i = 1;
		for (final Attribute attribute : recordIdentifierAP.getAttributes()) {

			sb.append("-[:`").append(attribute.getUri()).append("`]->");

			if (i < recordIdentifierAP.getAttributes().size()) {

				sb.append("()");
			}

			i++;
		}

		sb.append("(o)\n").append("WHERE n.").append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Resource).append("\" AND\nn.")
				.append(GraphStatics.DATA_MODEL_PROPERTY).append(" = \"").append(dataModelUri).append("\" AND\no.")
				.append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Literal).append("\"\nRETURN n.")
				.append(GraphStatics.URI_PROPERTY).append(" AS record_uri");

		return sb.toString();
	}

	private static String buildGetRecordUrisQuery(final String searchValue, final AttributePath keyAttributePath, final String dataModelUri) {

		final StringBuilder sb = new StringBuilder();

		sb.append("START o=node:").append(GraphIndexStatics.VALUES_INDEX_NAME).append("(").append(GraphStatics.VALUE).append("=\"")
				.append(searchValue).append("\")\nMATCH (n)");

		int i = 1;

		for (final Attribute attribute : keyAttributePath.getAttributes()) {

			sb.append("-[:`").append(attribute.getUri()).append("`]->");

			if (i < keyAttributePath.getAttributes().size()) {

				sb.append("()");
			}

			i++;
		}

		sb.append("(o)\n").append("WHERE n.").append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Resource).append("\" AND\nn.")
				.append(GraphStatics.DATA_MODEL_PROPERTY).append(" = \"").append(dataModelUri).append("\" AND\no.")
				.append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Literal).append("\"\nRETURN n.")
				.append(GraphStatics.URI_PROPERTY).append(" AS record_uri");

		return sb.toString();
	}

	private static String buildGetEntityLeafsQuery(final long nodeId) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append(GraphProcessingStatics.LEAF_IDENTIFIER).append("`)\nRETURN id(m) AS leaf_node");

		return sb.toString();
	}

	private static String buildGetEntityLeafsWithValueQuery(final long nodeId) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append(GraphProcessingStatics.LEAF_IDENTIFIER).append("`)\nRETURN id(m) AS leaf_node, m.")
				.append(GraphStatics.URI_PROPERTY).append(" AS leaf_uri, m.").append(GraphStatics.VALUE_PROPERTY).append(" AS leaf_value");

		return sb.toString();
	}

	private static String executeQueryWithSingleResult(final String query, final String resultVariableName, final GraphDatabaseService graphDB)
			throws DMPGraphException {

		String resultValue = null;

		try (final Transaction tx = graphDB.beginTx()) {

			final Result result = graphDB.execute(query);

			if (result != null) {

				if (result.hasNext()) {

					final Map<String, Object> row = result.next();

					Set<Map.Entry<String, Object>> entrySet = row.entrySet();

					final Iterator<Map.Entry<String, Object>> iter = entrySet.iterator();

					if (iter.hasNext()) {

						final Map.Entry<String, Object> column = iter.next();

						if (column.getValue() != null) {

							if (column.getKey().equals(resultVariableName)) {

								resultValue = column.getValue().toString();
							}
						}
					}
				}

				result.close();
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't execute query with single result";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		return resultValue;
	}

	public static Collection<String> executeQueryWithMultipleResults(final String query, final String resultVariableName,
			final GraphDatabaseService graphDB) throws DMPGraphException {

		final Set<String> resultSet = new HashSet<>();

		try (final Transaction tx = graphDB.beginTx()) {

			final Result result = graphDB.execute(query);

			if (result != null) {

				while (result.hasNext()) {

					final Map<String, Object> row = result.next();

					for (final Map.Entry<String, Object> column : row.entrySet()) {

						if (column.getValue() != null) {

							if (column.getKey().equals(resultVariableName)) {

								resultSet.add(column.getValue().toString());
							}
						}
					}
				}

				result.close();
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't execute query with multiple results";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		return resultSet;
	}

	private static Map<String, String> executeQueryWithMultipleResultsWithValues(final String query, final String resultVariableName,
			final String uriVariableName, final String valueVariableName, final GraphDatabaseService graphDB)
			throws DMPGraphException {

		final Map<String, String> resultSet = new HashMap<>();

		try (final Transaction tx = graphDB.beginTx()) {

			final Result result = graphDB.execute(query);

			if (result != null) {

				while (result.hasNext()) {

					final Map<String, Object> row = result.next();

					String identifier = null;
					String value = null;

					for (final Map.Entry<String, Object> column : row.entrySet()) {

						if (column.getValue() != null) {

							if (column.getKey().equals(resultVariableName)) {

								identifier = column.getValue().toString();
							}

							if (column.getKey().equals(uriVariableName) || column.getKey().equals(valueVariableName)) {

								value = column.getValue().toString();
							}
						}
					}

					if (identifier != null && value != null) {

						resultSet.put(identifier, value);
					}
				}

				result.close();
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't execute query with multiple results with values";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		return resultSet;
	}

	/**
	 * note: should be executed in transaction scope
	 *
	 * @param query
	 * @param resultVariableName
	 * @param graphDB
	 * @return
	 */
	public static Relationship executeQueryWithSingleRelationshipResult(final String query, final String resultVariableName,
			final GraphDatabaseService graphDB) {

		final Result result = graphDB.execute(query);

		if (result == null) {

			return null;
		}

		if (!result.hasNext()) {

			result.close();

			return null;
		}

		final Map<String, Object> row = result.next();

		final Iterator<Map.Entry<String, Object>> rowIter = row.entrySet().iterator();

		if (!rowIter.hasNext()) {

			result.close();

			return null;
		}

		final Map.Entry<String, Object> column = rowIter.next();

		if (column.getValue() == null) {

			result.close();

			return null;
		}

		if (!column.getKey().equals(resultVariableName) || !Relationship.class.isInstance(column.getValue())) {

			result.close();

			return null;
		}

		final Relationship rel = (Relationship) column.getValue();

		result.close();

		return rel;
	}
}
