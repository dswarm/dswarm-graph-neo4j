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
import java.util.Collections;
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
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
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
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.NodeType;
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
import org.dswarm.graph.utils.GraphUtils;

/**
 * @author tgaengler
 */
public final class GraphDBUtil {

	private static final Logger LOG = LoggerFactory.getLogger(GraphDBUtil.class);

	// http://www.w3.org/1999/02/22-rdf-syntax-ns#type
	public static final RelationshipType RDF_TYPE_REL_TYPE = DynamicRelationshipType.withName("rdf:type");

	public static final Optional<String> determineTypeLabel(final Node node) throws DMPGraphException {

		final Iterable<Label> labels = node.getLabels();

		if (labels == null) {

			throw new DMPGraphException(String.format("there are no labels at node %s", GraphDBPrintUtil.printNode(node)));
		}

		boolean nodeIsResource = false;

		for (final Label label : labels) {

			final String labelName = label.name();

			if (labelName.equals(GraphProcessingStatics.LEAF_IDENTIFIER)) {

				continue;
			}

			try {

				final NodeType nodeType = NodeType.getByName(labelName);

				if (NodeType.Resource.equals(nodeType)) {

					nodeIsResource = true;
				}
			} catch (final IllegalArgumentException e) {

				return Optional.of(labelName);
			}
		}

		if (nodeIsResource) {

			// object resources don't need a type label

			return Optional.absent();
		}

		throw new DMPGraphException(String.format("couldn't determine type label for node %s", GraphDBPrintUtil.printNode(node)));
	}

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
	 * @param prefixedResourceURI
	 * @return
	 */
	public static Node getResourceNode(final GraphDatabaseService graphDB, final String prefixedResourceURI) {

		final ResourceIterator<Node> resources = graphDB
				.findNodes(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.URI_PROPERTY, prefixedResourceURI);

		if (resources == null) {

			LOG.debug("couldn't find resource node for resource identifier '{}'", prefixedResourceURI);

			return null;
		}

		if (!resources.hasNext()) {

			LOG.debug("couldn't find resource node for resource identifier '{}'", prefixedResourceURI);

			resources.close();

			return null;
		}

		final Node resourceNode = resources.next();

		if (resources.hasNext()) {

			LOG.warn("there are more than one resource nodes for resource identifier '{}'", prefixedResourceURI);
		}

		resources.close();

		return resourceNode;
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param prefixedResourceURI
	 * @return
	 */
	public static Node getResourceNode(final GraphDatabaseService graphDB, final String prefixedResourceURI, final String prefixedDataModelURI) {

		final long resourceUriDataModelUriHash = HashUtils.generateHash(prefixedResourceURI + prefixedDataModelURI);

		return graphDB.findNode(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.HASH, resourceUriDataModelUriHash);
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
	 * @param prefixedResourceURI
	 * @return
	 */
	public static Iterable<Path> getResourcePaths(final GraphDatabaseService graphDB, final String prefixedResourceURI) {

		final Node resourceNode = getResourceNode(graphDB, prefixedResourceURI);

		// TODO: maybe replace with gethEntityPaths(GraphdataBaseService, Node)
		final Iterable<Path> paths = graphDB.traversalDescription().uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
				.order(BranchOrderingPolicies.POSTORDER_BREADTH_FIRST).expand(PathExpanderBuilder.allTypes(Direction.OUTGOING).build())
				.evaluator(new Evaluator() {

					@Override
					public Evaluation evaluate(final Path path) {

						final boolean hasLeafLabel = path.endNode() != null &&
								path.endNode().hasLabel(GraphProcessingStatics.LEAF_LABEL);

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
	 * @param prefixedResourceURI
	 * @param prefixedDataModelURI
	 * @return
	 */
	public static Iterable<Path> getResourcePaths(final GraphDatabaseService graphDB, final String prefixedResourceURI,
			final String prefixedDataModelURI) {

		final Node resourceNode = getResourceNode(graphDB, prefixedResourceURI, prefixedDataModelURI);

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
	public static boolean checkGraphMatchingCompleteness(final GraphDatabaseService graphDB, final String type) throws DMPGraphException {

		try (final Transaction tx = graphDB.beginTx()) {

			final Iterable<Relationship> rels = GlobalGraphOperations.at(graphDB).getAllRelationships();

			boolean incomplete = false;

			int positiveCounter = 0;
			int negativeCounter = 0;

			for (final Relationship rel : rels) {

				final Boolean relMatchedState = (Boolean) rel.getProperty(DeltaStatics.MATCHED_PROPERTY, null);
				final boolean finalRelMatchedState = checkMatchedState(relMatchedState, rel.getId(), DeltaStatics.RELATIONSHIP_TYPE);

				if (!finalRelMatchedState) {

					LOG.debug("couldn't mark relationship in {}: {}", type, GraphDBPrintUtil.printDeltaRelationship(rel));

					incomplete = true;

					negativeCounter++;

					continue;
				}

				final Boolean subjectMatchedState = (Boolean) rel.getStartNode().getProperty(DeltaStatics.MATCHED_PROPERTY, null);
				final boolean finalSubjectMatchedState = checkMatchedState(subjectMatchedState, rel.getStartNode().getId(), DeltaStatics.NODE_TYPE);

				if (!finalSubjectMatchedState) {

					LOG.debug("couldn't mark start node in {}: {}", type, GraphDBPrintUtil.printDeltaRelationship(rel));

					incomplete = true;

					negativeCounter++;

					continue;
				}

				final Boolean objectMatchedState = (Boolean) rel.getEndNode().getProperty(DeltaStatics.MATCHED_PROPERTY, null);
				final boolean finalObjectMatchedState = checkMatchedState(objectMatchedState, rel.getEndNode().getId(), DeltaStatics.NODE_TYPE);

				if (!finalObjectMatchedState) {

					LOG.debug("couldn't mark end node in {}: {}", type, GraphDBPrintUtil.printDeltaRelationship(rel));

					incomplete = true;

					negativeCounter++;

					continue;
				}

				positiveCounter++;
			}

			LOG.debug("marked '{}' relationships completely and missed '{}' ones in {}", positiveCounter, negativeCounter, type);

			final boolean result = !incomplete;

			tx.success();

			return result;
		} catch (final Exception e) {

			final String message = String.format("couldn't complete the graph matching completeness check for graph DB '%s'", graphDB);

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message, e);
		}
	}

	private static boolean checkMatchedState(final Boolean matchedState, final long id, final String type) {

		if (matchedState == null) {

			GraphDBUtil.LOG.error("{} '{}' couldn't be matched, i.e., there was no match state available", type, id);

			return false;
		}

		if (!matchedState) {

			GraphDBUtil.LOG.error("{} '{}' couldn't be matched, i.e., there was no match state was 'false'", type, id);

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
	public static boolean calculateEntityHash(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes)
			throws DMPGraphException {

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
	public static Long calculateNodeHash(final Node node) throws DMPGraphException {

		final NodeType nodeType = GraphUtils.determineNodeType(node);

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
	public static Long calculateSimpleNodeHash(final Node node) throws DMPGraphException {

		final NodeType nodeType = GraphUtils.determineNodeType(node);

		if (nodeType == null) {

			// // skip none typed nodes?
			return null;
		}

		return (long) nodeType.hashCode();
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
		final Node nodeById = graphDB.getNodeById(nodeId);
		final Iterable<Relationship> typeRels = nodeById.getRelationships(Direction.OUTGOING, RDF_TYPE_REL_TYPE);

		if (typeRels != null && typeRels.iterator().hasNext()) {

			for (final Relationship typeRel : typeRels) {

				// TODO: could be removed later
				GraphDBUtil.LOG.debug("fetch entity type rel: '{}'", typeRel.getId());

				GraphDBUtil.addNodeId(pathEndNodeIds, typeRel.getEndNode().getId());
			}
		} else {

			// TODO: looks like that this doesn't match anything at all

			final NodeType valueNodeType = GraphUtils.determineNodeType(nodeById);

			if (valueNodeType.equals(NodeType.BNode) || valueNodeType.equals(NodeType.TypeBNode)) {

				if (nodeById.hasLabel(GraphProcessingStatics.LEAF_LABEL)) {

					GraphDBUtil.LOG.debug("found leaf bnode {}", GraphDBPrintUtil.printNode(nodeById));

					GraphDBUtil.addNodeId(pathEndNodeIds, nodeId);
				}
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

		if (deltaState == DeltaState.ADDITION || deltaState == DeltaState.DELETION) {

			final Iterable<Path> nonMatchedSubGraphPaths = getNonMatchedSubGraphPaths(nodeId, graphDB);

			if (nonMatchedSubGraphPaths != null && nonMatchedSubGraphPaths.iterator().hasNext()) {

				for (final Path nonMatchtedSubGraphPath : nonMatchedSubGraphPaths) {

					GraphDBUtil.addNodeId(pathEndNodeIds, nonMatchtedSubGraphPath.endNode().getId());
				}
			}
		}
	}

	public static Collection<CSEntity> getCSEntities(final GraphDatabaseService graphDB, final String prefixedResourceURI,
			final AttributePath commonPrefixedAttributePath, final ContentSchema prefixedContentSchema)
			throws DMPGraphException {

		final Map<Long, CSEntity> csEntities = new LinkedHashMap<>();

		try (final Transaction tx = graphDB.beginTx()) {

			final Node resourceNode = getResourceNode(graphDB, prefixedResourceURI);

			// determine CS entity nodes
			final ResourceIterable<Node> csEntityNodes = graphDB.traversalDescription().breadthFirst()
					.evaluator(Evaluators.toDepth(commonPrefixedAttributePath.getAttributes().size()))
					.evaluator(new EntityEvaluator(commonPrefixedAttributePath.getAttributes()))
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

			if (prefixedContentSchema.getKeyAttributePaths() != null) {

				// determine key entities
				determineKeyEntities(graphDB, commonPrefixedAttributePath, prefixedContentSchema, csEntities, csEntityNodesArray);
			}

			if (prefixedContentSchema.getValueAttributePath() != null) {

				// determine value entities
				determineValueEntities(graphDB, commonPrefixedAttributePath, prefixedContentSchema, csEntities, csEntityNodesArray);
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
					// http://www.w3.org/1999/02/22-rdf-syntax-ns#type
					final Node endNode = nonMatchedRel.getEndNode();

					if ("rdf:type".equals(predicate)) {

						final long relId = nonMatchedRel.getId();
						final String value = (String) endNode.getProperty(GraphStatics.URI_PROPERTY, null);

						final String logout = "in determineNonMatchedCSEntitySubGraphs with rel id = '" + relId + "', value = '" + value + "'";
						System.out.println(logout);
						final String relPrint = GraphDBPrintUtil.printDeltaRelationship(nonMatchedRel);
						GraphDBUtil.LOG.debug(logout);
						GraphDBUtil.LOG.debug(relPrint);
					}

					final long endNodeId = endNode.getId();
					final int endNodeHierarchyLevel = (int) endNode.getProperty(DeltaStatics.HIERARCHY_LEVEL_PROPERTY);

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

	public static String determineRecordIdentifier(final GraphDatabaseService graphDB, final AttributePath prefixedRecordIdentifierAP,
			final String prefixedRecordURI)
			throws DMPGraphException {

		final String query = buildGetRecordIdentifierQuery(prefixedRecordIdentifierAP, prefixedRecordURI);

		return executeQueryWithSingleResult(query, "record_identifier", graphDB);
	}

	public static String determineRecordUri(final String recordId, final AttributePath prefixedRecordIdentifierAP, final String prefixedDataModelUri,
			final GraphDatabaseService graphDB) throws DMPGraphException {

		final String query = buildGetRecordUriQuery(recordId, prefixedRecordIdentifierAP, prefixedDataModelUri);
		return executeQueryWithSingleResult(query, "record_uri", graphDB);
	}

	public static Collection<String> determineRecordUris(final String searchValue, final AttributePath prefixedKeyAttributePath,
			final String prefixedDataModelUri,
			final GraphDatabaseService graphDB) throws DMPGraphException {

		final String query = buildGetRecordUrisQuery(searchValue, prefixedKeyAttributePath, prefixedDataModelUri);

		return executeQueryWithMultipleResults(query, "record_uri", graphDB);
	}

	public static Collection<ValueEntity> getFlatResourceNodeValues(final String prefixedResourceURI, final GraphDatabaseService graphDB)
			throws DMPGraphException {

		try (final Transaction tx = graphDB.beginTx()) {

			final Node resourceNode = getResourceNode(graphDB, prefixedResourceURI);

			final Collection<ValueEntity> flatResourceNodeValues = getFlatNodeValues(resourceNode, graphDB);

			tx.success();

			return flatResourceNodeValues;
		} catch (final Exception e) {

			final String message = "couldn't determine record uri";

			GraphDBUtil.LOG.error(message, e);

			throw new DMPGraphException(message, e);
		}
	}

	private static Collection<ValueEntity> getFlatNodeValues(final Node node, final GraphDatabaseService graphDB) throws DMPGraphException {

		if (node == null) {
			return Collections.emptyList();
		}
		// determine flat values
		final Iterable<Path> paths = graphDB.traversalDescription().breadthFirst().evaluator(new StatementEvaluator(node.getId())).traverse(node);

		if (paths == null || !paths.iterator().hasNext()) {

			return null;
		}

		final Map<String, CSEntity> valuesMap = new HashMap<>();

		for (final Path path : paths) {

			final Relationship rel = path.lastRelationship();
			final String predicate = rel.getType().name();
			final Node endNode = path.endNode();
			final long valueNodeId = endNode.getId();

			// TODO: are there any nodes without node type?
			final NodeType valueNodeType = GraphUtils.determineNodeType(endNode);
			final String value;

			switch (valueNodeType) {

				case Resource:
				case TypeResource:

					String tempValue = (String) endNode.getProperty(GraphStatics.URI_PROPERTY, null);

					final String dataModel = (String) endNode.getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

					if (dataModel != null) {

						tempValue += dataModel;
					}

					value = tempValue;

					break;
				case Literal:

					value = (String) endNode.getProperty(GraphStatics.VALUE_PROPERTY, null);

					break;
				default:

					value = null;
			}

			final Long valueOrder = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);

			final long finalValueOrder;

			if (valueOrder != null) {

				finalValueOrder = valueOrder;
			} else {

				finalValueOrder = 1;
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
			final ContentSchema prefixedContentSchema, final Map<Long, CSEntity> csEntities, final Node... csEntityNodesArray) {

		for (final AttributePath keyAttributePath : prefixedContentSchema.getKeyAttributePaths()) {

			final Optional<LinkedList<Attribute>> optionalRelativeKeyAttributePath = determineRelativeAttributePath(keyAttributePath,
					commonAttributePath);

			if (optionalRelativeKeyAttributePath.isPresent()) {

				final LinkedList<Attribute> relativeKeyAttributePath = optionalRelativeKeyAttributePath.get();

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
			} else {

				LOG.debug("couldn't determine relative key attribute path for key attribute path '{}' and common attribute path ''",
						keyAttributePath.toString(), commonAttributePath.toString());
			}
		}
	}

	private static void determineValueEntities(final GraphDatabaseService graphDB, final AttributePath commonAttributePath,
			final ContentSchema contentSchema, final Map<Long, CSEntity> csEntities, final Node... csEntityNodesArray) {

		final AttributePath valueAttributePath = contentSchema.getValueAttributePath();

		final Optional<LinkedList<Attribute>> optionalRelativeValueAttributePath = determineRelativeAttributePath(
				valueAttributePath,
				commonAttributePath);

		if (optionalRelativeValueAttributePath.isPresent()) {

			final LinkedList<Attribute> relativeValueAttributePath = optionalRelativeValueAttributePath.get();

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
		} else {

			LOG.debug("couldn't determine relative value attribute path for value attribute path '{}' and common attribute path ''",
					valueAttributePath.toString(), commonAttributePath.toString());
		}
	}

	private static Optional<LinkedList<Attribute>> determineRelativeAttributePath(final AttributePath attributePath,
			final AttributePath commonAttributePath) {

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

			return Optional.absent();
		}

		final LinkedList<Attribute> relativeAttributePath = new LinkedList<>();

		while (apIter.hasNext()) {

			relativeAttributePath.add(apIter.next());
		}

		return Optional.of(relativeAttributePath);
	}

	private static void determineCSEntityOrder(final Collection<CSEntity> csEntities) {

		final Group<CSEntity> keyGroup = Lambda.group(csEntities, Lambda.by(Lambda.on(CSEntity.class).getKey()));

		for (final Group<CSEntity> csEntityKeyGroup : keyGroup.subgroups()) {

			int i = 1;

			for (final CSEntity csEntity : csEntityKeyGroup.findAll()) {

				csEntity.setEntityOrder(i);
				i++;
			}
		}
	}

	private static String buildGetRecordIdentifierQuery(final AttributePath prefixedRecordIdentifierAP, final String prefixedRecordURI)
			throws DMPGraphException {

		//		MATCH (n:RESOURCE {uri;"ns2:18d68601-0623-42b4-ad89-f8954cc25912"})
		//      WITH n
		//		MATCH (n)-[:`oaipmh:header`]->()-[:`oaipmh:identifier`]->()-[:`rdf:value`]->(o:LITERAL)
		//		RETURN o.value AS record_identifier;

		final StringBuilder sb = new StringBuilder("MATCH ");

		sb.append("(n:").append(NodeType.Resource).append(" {").append(GraphStatics.URI_PROPERTY).append(":\"").append(prefixedRecordURI)
				.append("\"})\n")
				.append("WITH n\n")
				.append("MATCH ").append("(n)");

		final List<Attribute> attributes = prefixedRecordIdentifierAP.getAttributes();
		int i = attributes.size();

		for (final Attribute attribute : attributes) {

			final String prefixedAttributeUri = attribute.getUri();

			sb.append("-[:`").append(prefixedAttributeUri).append("`]->");

			if (--i > 0) {

				sb.append("()");
			}
		}

		sb.append("(o:").append(NodeType.Literal).append(")\n")
				.append("RETURN o.").append(GraphStatics.VALUE_PROPERTY).append(" AS record_identifier");

		return sb.toString();
	}

	private static String buildGetRecordUriQuery(
			final String recordId,
			final AttributePath prefixedRecordIdentifierAP,
			final String prefixedDataModelUri) throws DMPGraphException {

		// MATCH (o:LITERAL {value : "ID06978834"})
		// WITH o
		// MATCH (o)<-[:`mabxml:id`]-(n:RESOURCE)
		// WHERE n.datamodel = "ns1:1"
		// RETURN n.uri;

		final StringBuilder sb = new StringBuilder();
		sb.append("MATCH (o:").append(NodeType.Literal).append(" {").append(GraphStatics.VALUE_PROPERTY).append(":\"").append(recordId)
				.append("\"})\n")
				.append("WITH o\n")
				.append("MATCH (o)");

		final List<Attribute> attributes = prefixedRecordIdentifierAP.getAttributes();
		int i = attributes.size() - 1;

		while (i >= 0) {

			final Attribute attribute = attributes.get(i);
			final String prefixedAttributeURI = attribute.getUri();

			sb.append("<-[:`").append(prefixedAttributeURI).append("`]-");

			if (i > 0) {

				sb.append("()");
			}

			i--;
		}

		sb.append("(n:").append(NodeType.Resource).append(")\n")
				.append("WITH n\n")
				.append("MATCH n\n")
				.append("WHERE n.").append(GraphStatics.DATA_MODEL_PROPERTY).append(" = \"").append(prefixedDataModelUri).append("\"\n")
				.append("RETURN ").append("n.").append(GraphStatics.URI_PROPERTY).append(" AS record_uri");

		return sb.toString();
	}

	private static String buildGetRecordUrisQuery(final String searchValue, final AttributePath prefixedKeyAttributePath,
			final String prefixedDataModelUri)
			throws DMPGraphException {

		final StringBuilder sb = new StringBuilder();
		sb.append("MATCH (o:").append(NodeType.Literal).append(" {").append(GraphStatics.VALUE_PROPERTY).append(":\"").append(searchValue)
				.append("\"})\n")
				.append("WITH o\n")
				.append("MATCH (o)");

		final List<Attribute> attributes = prefixedKeyAttributePath.getAttributes();
		int i = attributes.size() - 1;

		while (i >= 0) {

			final Attribute attribute = attributes.get(i);
			final String prefixedAttributeURI = attribute.getUri();

			sb.append("<-[:`").append(prefixedAttributeURI).append("`]-");

			if (i > 0) {

				sb.append("()");
			}

			i--;
		}

		sb.append("(n:").append(NodeType.Resource).append(")\n")
				.append("WITH n\n")
				.append("MATCH n\n")
				.append("WHERE n.").append(GraphStatics.DATA_MODEL_PROPERTY).append(" = \"").append(prefixedDataModelUri).append("\"\n")
				.append("RETURN ").append("n.").append(GraphStatics.URI_PROPERTY).append(" AS record_uri");

		return sb.toString();
	}

	private static String buildGetEntityLeafsQuery(final long nodeId) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append(GraphProcessingStatics.LEAF_IDENTIFIER)
				.append("`)\nRETURN id(m) AS leaf_node");

		return sb.toString();
	}

	private static String buildGetEntityLeafsWithValueQuery(final long nodeId) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append(GraphProcessingStatics.LEAF_IDENTIFIER)
				.append("`)\nRETURN id(m) AS leaf_node, m.")
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

					final Set<Map.Entry<String, Object>> entrySet = row.entrySet();

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
