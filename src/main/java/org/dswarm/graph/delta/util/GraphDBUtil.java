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

import org.dswarm.graph.NodeType;
import org.dswarm.graph.delta.Attribute;
import org.dswarm.graph.delta.AttributePath;
import org.dswarm.graph.delta.ContentSchema;
import org.dswarm.graph.delta.DMPStatics;
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
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
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

import ch.lambdaj.Lambda;
import ch.lambdaj.group.Group;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * @author tgaengler
 */
public final class GraphDBUtil {

	private static final Logger				LOG				= LoggerFactory.getLogger(GraphDBUtil.class);

	private static final RelationshipType	rdfTypeRelType	= DynamicRelationshipType.withName("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param resourceURI
	 * @return
	 */
	public static Node getResourceNode(final GraphDatabaseService graphDB, final String resourceURI) {

		final Index<Node> resources = graphDB.index().forNodes("resources");

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

						final boolean hasLeafLabel = path.endNode().hasLabel(DMPStatics.LEAF_LABEL);

						if (hasLeafLabel) {

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
	public static boolean checkGraphMatchingCompleteness(final GraphDatabaseService graphDB) {

		final Transaction tx = graphDB.beginTx();

		try {

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

			tx.failure();

			GraphDBUtil.LOG.error("couldn't complete the graph matching completeness check for graph DB '" + graphDB.toString() + "'", e);
		} finally {

			tx.close();
		}

		return false;
	}

	private static boolean checkMatchedState(final Boolean matchedState, final long id, final String type) {

		if (matchedState == null) {

			GraphDBUtil.LOG.debug(type + " '" + id + "' couldn't be matched, i.e., there was no match state available");

			return false;
		}

		if (!matchedState) {

			GraphDBUtil.LOG.debug(type + " '" + id + "' couldn't be matched, i.e., there was no match state was 'false'");

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

						final boolean hasLeafLabel = path.endNode().hasLabel(DMPStatics.LEAF_LABEL);

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

						final boolean hasLeafLabel = path.endNode().hasLabel(DMPStatics.LEAF_LABEL);

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

				final String provenance = (String) node.getProperty(GraphStatics.PROVENANCE_PROPERTY, null);

				if (provenance != null) {

					tempValue += provenance;
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
	public static Collection<String> getEntityLeafs(final GraphDatabaseService graphDB, final long nodeId) {

		final String entityLeafsQuery = buildGetEntityLeafsQuery(nodeId);

		return executeQueryWithMultipleResults(entityLeafsQuery, "leaf_node", graphDB);
	}

	/**
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	private static Map<String, String> getEntityLeafsWithValue(final GraphDatabaseService graphDB, final long nodeId) {

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
	public static void fetchEntityTypeNodes(final GraphDatabaseService graphDB, final Set<Long> pathEndNodeIds, final long nodeId) {

		// fetch type nodes as well
		final Iterable<Relationship> typeRels = graphDB.getNodeById(nodeId).getRelationships(Direction.OUTGOING, rdfTypeRelType);

		if (typeRels != null && typeRels.iterator().hasNext()) {

			for (final Relationship typeRel : typeRels) {

				// TODO: could be removed later
				GraphDBUtil.LOG.debug("fetch entity type rel: '" + typeRel.getId() + "'");

				pathEndNodeIds.add(typeRel.getEndNode().getId());
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
			final Set<Long> pathEndNodeIds, final long nodeId) {

		if (deltaState.equals(DeltaState.ADDITION) || deltaState.equals(DeltaState.DELETION)) {

			final Iterable<Path> nonMatchedSubGraphPaths = getNonMatchedSubGraphPaths(nodeId, graphDB);

			if (nonMatchedSubGraphPaths != null && nonMatchedSubGraphPaths.iterator().hasNext()) {

				for (final Path nonMatchtedSubGraphPath : nonMatchedSubGraphPaths) {

					pathEndNodeIds.add(nonMatchtedSubGraphPath.endNode().getId());
				}
			}
		}
	}

	public static Collection<CSEntity> getCSEntities(final GraphDatabaseService graphDB, final String resourceURI, final AttributePath commonAttributePath, final ContentSchema contentSchema) {

		final Map<Long, CSEntity> csEntities = new LinkedHashMap<>();
		
		final Transaction tx = graphDB.beginTx();

		try {

			final Node resourceNode = getResourceNode(graphDB, resourceURI);

			// determine CS entity nodes
			final ResourceIterable<Node> csEntityNodes = graphDB.traversalDescription().breadthFirst()
					.evaluator(Evaluators.toDepth(commonAttributePath.getAttributes().size())).evaluator(new EntityEvaluator(commonAttributePath.getAttributes()))
					.traverse(resourceNode).nodes();

			if(csEntityNodes == null) {

				tx.success();

				return null;
			}

			for(final Node node : csEntityNodes) {

				final CSEntity csEntity = new CSEntity(node.getId());
				csEntities.put(csEntity.getNodeId(), csEntity);
			}

			final ArrayList<Node> csEntityNodesList = Lists.newArrayList(csEntityNodes);
			final Node[] csEntityNodesArray = new Node[csEntityNodesList.size()];
			csEntityNodesList.toArray(csEntityNodesArray);

			if(contentSchema.getKeyAttributePaths() != null) {

				// determine key entities
				determineKeyEntities(graphDB, commonAttributePath, contentSchema, csEntities, csEntityNodesArray);
			}

			if(contentSchema.getValueAttributePath() != null) {

				// determine value entities
				determineValueEntities(graphDB, commonAttributePath, contentSchema, csEntities, csEntityNodesArray);
			}

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBUtil.LOG.error("couldn't determine cs entities successfully", e);
		} finally {

			tx.close();
		}

		final Collection<CSEntity> csEntitiesCollection = csEntities.values();

		// determine cs entity order
		determineCSEntityOrder(csEntitiesCollection);

		return csEntitiesCollection;
	}

	public static Optional<? extends Collection<SubGraphLeafEntity>> getSubGraphLeafEntities(final Optional<? extends Collection<SubGraphEntity>> subGraphEntities, final GraphDatabaseService graphDB) {

		if(!subGraphEntities.isPresent()) {

			return Optional.absent();
		}

		final Set<SubGraphLeafEntity> subGraphLeafEntities = new HashSet<>();

		for(final SubGraphEntity subGraphEntity : subGraphEntities.get()) {

			final Map<String, String> subGraphLeafs = GraphDBUtil.getEntityLeafsWithValue(graphDB, subGraphEntity.getNodeId());

			if(subGraphLeafs == null || subGraphLeafs.isEmpty()) {

				continue;
			}

			for(final Map.Entry<String, String> subGraphLeafEntry : subGraphLeafs.entrySet()) {

				final SubGraphLeafEntity subGraphLeafEntity = new SubGraphLeafEntity(Long.valueOf(subGraphLeafEntry.getKey()), subGraphLeafEntry.getValue(), subGraphEntity);

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

						if (path.lastRelationship() == null && path.endNode().hasLabel(DMPStatics.LEAF_LABEL)) {

							return Evaluation.EXCLUDE_AND_PRUNE;
						}

						if (path.lastRelationship() == null) {

							return Evaluation.EXCLUDE_AND_CONTINUE;
						}

						if (path.lastRelationship().hasProperty(DeltaStatics.MATCHED_PROPERTY)) {

							// include only non-matched relationships (paths)
							return Evaluation.EXCLUDE_AND_PRUNE;
						}

						final boolean hasLeafLabel = path.endNode().hasLabel(DMPStatics.LEAF_LABEL);

						if (hasLeafLabel) {

							return Evaluation.INCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_CONTINUE;
					}
				}).traverse(entityNode);

		return paths;
	}

	public static Collection<SubGraphEntity> determineNonMatchedCSEntitySubGraphs(final Collection<CSEntity> csEntities, final GraphDatabaseService graphDB) {

		final Set<SubGraphEntity> subgraphEntities = new HashSet<>();

		final Transaction tx = graphDB.beginTx();

		try {

			for(final CSEntity csEntity : csEntities) {

				final Node csEntityNode = graphDB.getNodeById(csEntity.getNodeId());
				final Iterable<Relationship> csEntityOutgoingRels = csEntityNode.getRelationships(Direction.OUTGOING);

				if(csEntityOutgoingRels == null || !csEntityOutgoingRels.iterator().hasNext()) {

					continue;
				}

				final List<Relationship> nonMatchedRels = new ArrayList<>();

				for(final Relationship csEntityOutgoingRel : csEntityOutgoingRels) {

					if(csEntityOutgoingRel.hasProperty(DeltaStatics.MATCHED_PROPERTY)) {

						continue;
					}

					nonMatchedRels.add(csEntityOutgoingRel);
				}

				if(nonMatchedRels.isEmpty()) {

					// everything is already matched
					continue;
				}

				final String deltaStateString = (String) csEntityNode.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);
				final DeltaState deltaState;

				if(deltaStateString != null) {

					deltaState = DeltaState.getByName(deltaStateString);
				} else {

					deltaState = null;
				}

				for(final Relationship nonMatchedRel : nonMatchedRels) {

					final String predicate = nonMatchedRel.getType().name();
					final Long order = (Long) nonMatchedRel.getProperty(GraphStatics.ORDER_PROPERTY, null);
					final long finalOrder;

					if(order != null) {

						finalOrder = order;
					} else {

						finalOrder = 1;
					}

					// TODO: remove this later, it'S just for debugging purpose right now
					if(predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {

						final long relId = nonMatchedRel.getId();
						final String value = (String) nonMatchedRel.getEndNode().getProperty(GraphStatics.URI_PROPERTY, null);

						final String logout = "in determineNonMatchedCSEntitySubGraphs with rel id = '" + relId + "', value = '" + value +"'";
						System.out.println(logout);
						final String relPrint = GraphDBPrintUtil.printDeltaRelationship(nonMatchedRel);
						GraphDBUtil.LOG.debug(logout);
						GraphDBUtil.LOG.debug(relPrint);
					}

					final long endNodeId = nonMatchedRel.getEndNode().getId();
					final int endNodeHierarchyLevel = (int) nonMatchedRel.getEndNode().getProperty("__HIERARCHY_LEVEL__");

					final SubGraphEntity subGraphEntity = new SubGraphEntity(endNodeId, predicate, deltaState, csEntity, finalOrder, endNodeHierarchyLevel);
					subgraphEntities.add(subGraphEntity);
				}
			}

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBUtil.LOG.error("couldn't determine non-matched cs entity sub graphs", e);
		} finally {

			tx.close();
		}

		return subgraphEntities;
	}

	public static String determineRecordIdentifier(final GraphDatabaseService graphDB, final AttributePath recordIdentifierAP, final String recordURI) {

		final String query = buildGetRecordIdentifierQuery(recordIdentifierAP, recordURI);

		return executeQueryWithSingleResult(query, "record_identifier", graphDB);
	}

	public static String determineRecordUri(final String recordId, final AttributePath recordIdentifierAP, final String resourceGraphUri,
			final GraphDatabaseService graphDB) {

		final String query = buildGetRecordUriQuery(recordId, recordIdentifierAP, resourceGraphUri);

		return executeQueryWithSingleResult(query, "record_uri", graphDB);
	}

	public static Collection<ValueEntity> getFlatResourceNodeValues(final String resourceURI, final GraphDatabaseService graphDB) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Node resourceNode = getResourceNode(graphDB, resourceURI);

			final Collection<ValueEntity> flatResourceNodeValues = getFlatNodeValues(resourceNode, graphDB);

			tx.success();

			return flatResourceNodeValues;
		} catch (final Exception e) {

			tx.failure();

			GraphDBUtil.LOG.error("couldn't determine record uri", e);
		} finally {

			tx.close();
		}

		return null;
	}

	private static Collection<ValueEntity> getFlatNodeValues(final Node node, final GraphDatabaseService graphDB) {

		// determine flat values
		final Iterable<Path> paths = graphDB.traversalDescription().breadthFirst().evaluator(new StatementEvaluator(node.getId())).traverse(node);

		if(paths == null || !paths.iterator().hasNext()) {

			return null;
		}

		final Map<String, CSEntity> valuesMap = new HashMap<>();

		for(final Path path : paths) {

			final Relationship rel = path.lastRelationship();
			final String predicate = rel.getType().name();
			final long valueNodeId = path.endNode().getId();

			final String nodeTypeString = (String) path.endNode().getProperty(GraphStatics.NODETYPE_PROPERTY, null);

			if(nodeTypeString == null) {

				// skip none typed nodes?
				continue;
			}

			final NodeType valueNodeType = NodeType.getByName(nodeTypeString);
			final String value;

			switch(valueNodeType) {

				case Resource:
				case TypeResource:

					String tempValue = (String) path.endNode().getProperty(GraphStatics.URI_PROPERTY, null);

					final String provenance = (String) path.endNode().getProperty(GraphStatics.PROVENANCE_PROPERTY, null);

					if(provenance != null) {

						tempValue += provenance;
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

			if(valueOrder != null) {

				finalValueOrder = valueOrder;
			} else {

				finalValueOrder = (long) 1;
			}

			final GDMValueEntity valueEntity = new GDMValueEntity(valueNodeId, value, finalValueOrder, valueNodeType);

			if(!valuesMap.containsKey(predicate)) {

				final CSEntity csEntity = new CSEntity(-1);
				final KeyEntity keyEntity = new KeyEntity(-1, predicate);
				csEntity.addKeyEntity(keyEntity);

				valuesMap.put(predicate, csEntity);
			}

			valuesMap.get(predicate).addValueEntity(valueEntity);
		}

		final List<ValueEntity> values = new ArrayList<>();

		for(final CSEntity csEntity : valuesMap.values()) {

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

		while(apIter.hasNext()) {

			if(!commonAPIter.hasNext()) {

				break;
			}

			final Attribute apAttribute = apIter.next();
			final Attribute commonAttribute = commonAPIter.next();

			if(!apAttribute.equals(commonAttribute)) {

				break;
			}
		}

		if(!apIter.hasNext()) {

			return null;
		}

		final LinkedList<Attribute> relativeAttributePath = new LinkedList<>();

		while(apIter.hasNext()) {

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

		sb.append("START n=node:resources(").append(GraphStatics.URI).append("=\"").append(recordURI).append("\")\nMATCH (n)");

		int i = 1;
		for (final Attribute attribute : recordIdentifierAP.getAttributes()) {

			sb.append("-[:`").append(attribute.getUri()).append("`]->");

			if (i < recordIdentifierAP.getAttributes().size()) {

				sb.append("()");
			}
		}

		sb.append("(o)\n").append("WHERE n.").append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Resource).append("\" AND\no.")
				.append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Literal).append("\"\nRETURN o.")
				.append(GraphStatics.VALUE_PROPERTY).append(" AS record_identifier");

		return sb.toString();
	}

	private static String buildGetRecordUriQuery(final String recordId, final AttributePath recordIdentifierAP, final String resourceGraphUri) {

		// START n=node:values(__VALUE__="a1280f78-5f96-4fe6-b916-5e38e5d620d3")
		// MATCH (n)-[r:`http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#id`]->(o)
		// WHERE n.__NODETYPE__ = "__RESOURCE__" AND
		// o.__NODETYPE__ = "__LITERAL__"
		// RETURN o.__URI__ AS record_uri;

		final StringBuilder sb = new StringBuilder();

		sb.append("START o=node:values(").append(GraphStatics.VALUE).append("=\"").append(recordId).append("\")\nMATCH (n)");

		int i = 1;
		for (final Attribute attribute : recordIdentifierAP.getAttributes()) {

			sb.append("-[:`").append(attribute.getUri()).append("`]->");

			if (i < recordIdentifierAP.getAttributes().size()) {

				sb.append("()");
			}
		}

		sb.append("(o)\n").append("WHERE n.").append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Resource).append("\" AND\nn.")
				.append(GraphStatics.PROVENANCE_PROPERTY).append(" = \"").append(resourceGraphUri).append("\" AND\no.")
				.append(GraphStatics.NODETYPE_PROPERTY).append(" = \"").append(NodeType.Literal).append("\"\nRETURN n.")
				.append(GraphStatics.URI_PROPERTY).append(" AS record_uri");

		return sb.toString();
	}

	private static String buildGetEntityLeafsQuery(final long nodeId) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append("__LEAF__").append("`)\nRETURN id(m) AS leaf_node");

		return sb.toString();
	}

	private static String buildGetEntityLeafsWithValueQuery(final long nodeId) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append("__LEAF__").append("`)\nRETURN id(m) AS leaf_node, m.")
				.append(GraphStatics.URI_PROPERTY).append(" AS leaf_uri, m.").append(GraphStatics.VALUE_PROPERTY).append(" AS leaf_value");

		return sb.toString();
	}

	private static String executeQueryWithSingleResult(final String query, final String resultVariableName, final GraphDatabaseService graphDB) {

		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result;
		String resultValue = null;

		final Transaction tx = graphDB.beginTx();

		try {

			result = engine.execute(query);

			if (result != null) {

				for (final Map<String, Object> row : result) {

					for (final Map.Entry<String, Object> column : row.entrySet()) {

						if (column.getValue() != null) {

							if (column.getKey().equals(resultVariableName)) {

								resultValue = column.getValue().toString();
							}
						}

						break;
					}

					break;
				}
			}

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBUtil.LOG.error("couldn't execute query with single result", e);
		} finally {

			tx.close();
		}

		return resultValue;
	}

	public static Collection<String> executeQueryWithMultipleResults(final String query, final String resultVariableName,
			final GraphDatabaseService graphDB) {

		final Set<String> resultSet = new HashSet<>();
		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result;

		final Transaction tx = graphDB.beginTx();

		try {

			result = engine.execute(query);

			if(result != null) {

				for (final Map<String, Object> row : result) {

					for (final Map.Entry<String, Object> column : row.entrySet()) {

						if (column.getValue() != null) {

							if (column.getKey().equals(resultVariableName)) {

								resultSet.add(column.getValue().toString());
							}
						}
					}
				}
			}

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBUtil.LOG.error("couldn't execute query with multiple results", e);
		} finally {

			tx.close();
		}

		return resultSet;
	}

	private static Map<String, String> executeQueryWithMultipleResultsWithValues(final String query, final String resultVariableName, final String uriVariableName, final String valueVariableName, final GraphDatabaseService graphDB) {

		final Map<String, String> resultSet = new HashMap<>();
		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result;

		final Transaction tx = graphDB.beginTx();

		try {

			result = engine.execute(query);

			if(result != null) {

				for (final Map<String, Object> row : result) {

					String identifier = null;
					String value = null;

					for (final Map.Entry<String, Object> column : row.entrySet()) {

						if (column.getValue() != null) {

							if (column.getKey().equals(resultVariableName)) {

								identifier = column.getValue().toString();
							}

							if(column.getKey().equals(uriVariableName) || column.getKey().equals(valueVariableName)) {

								value = column.getValue().toString();
							}
						}
					}

					if(identifier != null && value != null) {

						resultSet.put(identifier, value);
					}
				}
			}

			tx.success();
		} catch (final Exception e) {

			tx.failure();

			GraphDBUtil.LOG.error("couldn't execute query with multiple results with values", e);
		} finally {

			tx.close();
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

		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result = engine.execute(query);

		if (result == null) {

			return null;
		}

		final ResourceIterator<Map<String, Object>> resultIter = result.iterator();

		if (resultIter == null || !resultIter.hasNext()) {

			if (resultIter != null) {

				resultIter.close();
			}

			return null;
		}

		final Map<String, Object> row = resultIter.next();

		final Iterator<Map.Entry<String, Object>> rowIter = row.entrySet().iterator();

		if (!rowIter.hasNext()) {

			resultIter.close();

			return null;
		}

		final Map.Entry<String, Object> column = rowIter.next();

		if (column.getValue() == null) {

			resultIter.close();

			return null;
		}

		if (!column.getKey().equals(resultVariableName) || !Relationship.class.isInstance(column.getValue())) {

			resultIter.close();

			return null;
		}

		final Relationship rel = (Relationship) column.getValue();

		resultIter.close();

		return rel;
	}
}
