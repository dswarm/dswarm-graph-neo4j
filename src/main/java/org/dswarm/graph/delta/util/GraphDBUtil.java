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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;

import ch.lambdaj.Lambda;
import ch.lambdaj.group.Group;

import com.google.common.collect.Lists;

/**
 * Created by tgaengler on 29/07/14.
 */
public final class GraphDBUtil {

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

			return null;
		}

		return hits.next();
	}

	public static void printNodes(final GraphDatabaseService graphDB) {

		Transaction tx = graphDB.beginTx();

		final Iterable<Node> nodes = GlobalGraphOperations.at(graphDB).getAllNodes();

		for (final Node node : nodes) {

			final Iterable<Label> labels = node.getLabels();

			for (final Label label : labels) {

				System.out.println("node = '" + node.getId() + "' :: label = '" + label.name());
			}

			final Iterable<String> propertyKeys = node.getPropertyKeys();

			for (final String propertyKey : propertyKeys) {

				final Object value = node.getProperty(propertyKey);

				System.out.println("node = '" + node.getId() + "' :: key = '" + propertyKey + "' :: value = '" + value + "'");
			}
		}

		tx.success();
		tx.close();
	}

	public static void printRelationships(final GraphDatabaseService graphDB) {

		Transaction tx = graphDB.beginTx();

		final Iterable<Relationship> relationships = GlobalGraphOperations.at(graphDB).getAllRelationships();

		for (final Relationship relationship : relationships) {

			final RelationshipType type = relationship.getType();

			System.out.println("relationship = '" + relationship.getId() + "' :: relationship type = '" + type.name());

			final Iterable<String> propertyKeys = relationship.getPropertyKeys();

			for (final String propertyKey : propertyKeys) {

				final Object value = relationship.getProperty(propertyKey);

				System.out.println("relationship = '" + relationship.getId() + "' :: key = '" + propertyKey + "' :: value = '" + value + "'");
			}
		}

		tx.success();
		tx.close();
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

	public static void calculateSubGraphEntityHash(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes) {

		try (final Transaction ignored = graphDB.beginTx()) {

			calculateEntityHash(graphDB, entityNodeId, nodeHashes);
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param graphDB
	 * @param entityNodeId
	 * @param nodeHashes
	 * @return
	 */
	private static boolean calculateEntityHash(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes) {

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
	private static long calculateRelationshipHash(long hash, final Relationship rel, final Long endNodeHash) {

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
	private static Long calculateNodeHash(final Node node) {

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
	private static Long calculateSimpleNodeHash(final Node node) {

		final NodeType nodeType = getNodeType(node);

		if (nodeType == null) {

			// // skip none typed nodes?
			return null;
		}

		final String value = getValue(node, nodeType);

		final long leafNodeHash = nodeType.hashCode();

		return leafNodeHash;
	}

	public static void calculateEntityHierarchyLevelNodesHashes(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes, final int hierarchyLevel) {

		final Collection<String> entityHierarchyLevelNodeIds = getEntityHierarchyLevelNodes(graphDB, entityNodeId, hierarchyLevel);

		try (final Transaction ignored = graphDB.beginTx()) {

			for(final String entityHierarchyLevelNodeId : entityHierarchyLevelNodeIds) {

				calculateEntityHash(graphDB, Long.valueOf(entityHierarchyLevelNodeId), nodeHashes);
			}
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}
	}

	public static Integer calculateEntityLeafHashes(final GraphDatabaseService graphDB, final long entityNodeId, final Map<Long, Long> nodeHashes) {

		final Collection<String> entityLeafNodeIds = getEntityLeafs(graphDB, entityNodeId);
		Integer deepestHierarchyLevel = null;

		try (final Transaction ignored = graphDB.beginTx()) {

			for(final String entityLeafNodeId : entityLeafNodeIds) {

				final Node leafNode = graphDB.getNodeById(Long.valueOf(entityLeafNodeId));


				Long hash = calculateNodeHash(leafNode);

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

			// TODO: log something

			e.printStackTrace();
		}

		return deepestHierarchyLevel;
	}

	public static Long calculateSubGraphLeafPathHash(final Long leafNodeId, final Long entityNodeId, final GraphDatabaseService graphDB) {

		try (final Transaction ignored = graphDB.beginTx()) {

			final Iterable<Path> entityPaths =  GraphDBUtil.getEntityPaths(graphDB, entityNodeId, leafNodeId);
			if(entityPaths == null || !entityPaths.iterator().hasNext()) {

				return null;
			}

			final Path entityPath = entityPaths.iterator().next();
			final Iterator<Node> entityPathNodes = entityPath.reverseNodes().iterator();

			Long endNodeHash = calculateNodeHash(entityPathNodes.next());

			if(endNodeHash == null) {

				return null;
			}

			Long subGraphLeafPathHash = (long) 0;

			final Iterable<Relationship> entityPathRels = entityPath.reverseRelationships();

			for(final Relationship entityPathRel : entityPathRels) {

				subGraphLeafPathHash = calculateRelationshipHash(subGraphLeafPathHash, entityPathRel, endNodeHash);

				final Node endNode = entityPathNodes.next();
				endNodeHash = calculateNodeHash(endNode);
			}

			return subGraphLeafPathHash;
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		return null;
	}

	public static Long calculateSubGraphLeafPathModificationHash(final Long leafNodeId, final Long entityNodeId, final GraphDatabaseService graphDB) {

		try (final Transaction ignored = graphDB.beginTx()) {

			final Iterable<Path> entityPaths =  GraphDBUtil.getEntityPaths(graphDB, entityNodeId, leafNodeId);

			if(entityPaths == null || !entityPaths.iterator().hasNext()) {

				return null;
			}

			final Path entityPath = entityPaths.iterator().next();
			final Iterator<Node> entityPathNodes = entityPath.reverseNodes().iterator();

			Long endNodeHash = calculateSimpleNodeHash(entityPathNodes.next());

			if(endNodeHash == null) {

				return null;
			}

			Long subGraphLeafPathHash = (long) 0;

			final Iterable<Relationship> entityPathRels = entityPath.reverseRelationships();

			for(final Relationship entityPathRel : entityPathRels) {

				subGraphLeafPathHash = calculateRelationshipHash(subGraphLeafPathHash, entityPathRel, endNodeHash);

				final Node endNode = entityPathNodes.next();
				endNodeHash = calculateNodeHash(endNode);
			}

			return subGraphLeafPathHash;
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		return null;
	}

	private static NodeType getNodeType(final Node node) {

		final String nodeTypeString = (String) node.getProperty(GraphStatics.NODETYPE_PROPERTY, null);

		if (nodeTypeString == null) {

			return null;
		}

		final NodeType nodeType = NodeType.getByName(nodeTypeString);

		return nodeType;
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
	private static Collection<String> getEntityHierarchyLevelNodes(final GraphDatabaseService graphDB, final long nodeId, final int hierarchyLevel) {

		final String entitHierarchyLevelNodesQuery = buildGetEntityHierarchyLevelNodesQuery(nodeId, hierarchyLevel);
		final Collection<String> entityHierarchyLevelNodeIds = executeQueryWithMultipleResults(entitHierarchyLevelNodesQuery, "hierarchy_level_node",
				graphDB);

		return entityHierarchyLevelNodeIds;
	}

	/**
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	private static Collection<String> getEntityLeafs(final GraphDatabaseService graphDB, final long nodeId) {

		final String entityLeafsQuery = buildGetEntityLeafsQuery(nodeId);
		final Collection<String> entityLeafNodeIds = executeQueryWithMultipleResults(entityLeafsQuery, "leaf_node", graphDB);

		return entityLeafNodeIds;
	}

	/**
	 * @param graphDB
	 * @param nodeId
	 * @return
	 */
	private static Map<String, String> getEntityLeafsWithValue(final GraphDatabaseService graphDB, final long nodeId) {

		final String entityLeafsQuery = buildGetEntityLeafsWithValueQuery(nodeId);
		final Map<String, String> entityLeafs = executeQueryWithMultipleResultsWithValues(entityLeafsQuery, "leaf_node", "leaf_uri", "leaf_value", graphDB);

		return entityLeafs;
	}

	public static void printPaths(final GraphDatabaseService graphDB, final String resourceURI) {

		final Transaction tx = graphDB.beginTx();

		final Iterable<Path> paths = getResourcePaths(graphDB, resourceURI);
		printPaths(paths);

		tx.success();
		tx.close();
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param paths
	 */
	public static void printPaths(final Iterable<Path> paths) {

		final Traversal.PathDescriptor<Path> pathPrinter = new PathPrinter();

		for (final Path path : paths) {

			final String pathString = Traversal.pathToString(path, pathPrinter);

			System.out.println(pathString);
		}
	}

	public static void printEntityPaths(final GraphDatabaseService graphDB, final long nodeId) {

		final Transaction tx = graphDB.beginTx();

		final Iterable<Path> paths = getEntityPaths(graphDB, nodeId);
		printPaths(paths);

		tx.success();
		tx.close();
	}

	public static void markCSEntityPaths(final Collection<CSEntity> matchedCSEntities, final DeltaState deltaState, final GraphDatabaseService graphDB, final String resourceURI) {

		final Set<Long> pathEndNodeIds = new HashSet<>();
		final Map<CSEntity, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();
		final Map<CSEntity, Set<Long>> modifiedPathEndNodesIdsFromCSEntityMap = new HashMap<>();

		try (final Transaction ignored = graphDB.beginTx()) {

			// calc path end nodes
			for (final CSEntity csEntity : matchedCSEntities) {

				// TODO: mark type nodes from other paths as well?

				for (final KeyEntity keyEntity : csEntity.getKeyEntities()) {

					pathEndNodeIds.add(keyEntity.getNodeId());
				}

				for (final ValueEntity valueEntity : csEntity.getValueEntities()) {

					if(!deltaState.equals(DeltaState.MODIFICATION)) {

						pathEndNodeIds.add(valueEntity.getNodeId());
					} else {

						if(!modifiedPathEndNodesIdsFromCSEntityMap.containsKey(csEntity)) {

							modifiedPathEndNodesIdsFromCSEntityMap.put(csEntity, new HashSet<Long>());
						}

						modifiedPathEndNodesIdsFromCSEntityMap.get(csEntity).add(valueEntity.getNodeId());
					}
				}

				final Set<Long> pathEndNodeIdsFromCSEntity = new HashSet<>();

				//markEntityTypeNodes(graphDB, deltaState, csEntity.getNodeId());
				fetchEntityTypeNodes(graphDB, pathEndNodeIdsFromCSEntity, csEntity.getNodeId());

				determineNonMatchedSubGraphPathEndNodes(deltaState, graphDB, pathEndNodeIdsFromCSEntity, csEntity.getNodeId());

				pathEndNodesIdsFromCSEntityMap.put(csEntity, pathEndNodeIdsFromCSEntity);
			}
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		final DeltaState finalDeltaState;

		if(!deltaState.equals(DeltaState.MODIFICATION)) {

			finalDeltaState = deltaState;
		} else {

			finalDeltaState = DeltaState.ExactMatch;
		}

		markPaths(finalDeltaState, graphDB, resourceURI, pathEndNodeIds);

		for(final Map.Entry<CSEntity, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			markPaths(finalDeltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(), pathEndNodeIdsFromCSEntityEntry.getValue());
		}

		for(final Map.Entry<CSEntity, Set<Long>> modifiedPathEndNodeIdsFromCSEntityEntry : modifiedPathEndNodesIdsFromCSEntityMap.entrySet()) {

			markPaths(deltaState, graphDB, modifiedPathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(), modifiedPathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}

	/**
	 * note: we may need to find a better way to handle those statements
	 *
	 * @param graphDB
	 * @param nodeId
	 */
	private static void markEntityTypeNodes(final GraphDatabaseService graphDB, final DeltaState deltaState, final long nodeId) {

		final Set<Long> pathEndNodeIds = new HashSet<>();

		fetchEntityTypeNodes(graphDB, pathEndNodeIds, nodeId);
		markPaths(deltaState, graphDB, nodeId, pathEndNodeIds);
	}

	/**
	 * @param graphDB
	 * @param pathEndNodeIds
	 * @param nodeId
	 */
	private static void fetchEntityTypeNodes(final GraphDatabaseService graphDB, final Set<Long> pathEndNodeIds, final long nodeId) {

		// fetch type nodes as well
		final Iterable<Relationship> typeRels = graphDB.getNodeById(nodeId).getRelationships(Direction.OUTGOING, rdfTypeRelType);

		if (typeRels != null && typeRels.iterator().hasNext()) {

			for (final Relationship typeRel : typeRels) {

				pathEndNodeIds.add(typeRel.getEndNode().getId());
			}
		}
	}

	/**
	 * note: we could also include everything from a sub entity that was marked as deleted or added, since then the whole sub
	 * entity is affected to this state
	 *
	 * @param matchedValueEntities
	 * @param deltaState
	 * @param graphDB
	 * @param resourceURI
	 */
	public static void markValueEntityPaths(final Collection<ValueEntity> matchedValueEntities, final DeltaState deltaState,
			final GraphDatabaseService graphDB, final String resourceURI) {

		final Set<Long> pathEndNodeIds = new HashSet<>();
		final Map<CSEntity, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();
		final Map<CSEntity, Set<Long>> modifiedPathEndNodesIdsFromCSEntityMap = new HashMap<>();

		try (final Transaction ignored = graphDB.beginTx()) {

			// calc path end nodes
			for(final ValueEntity valueEntity : matchedValueEntities) {

				// TODO: what should I do with related key paths here?
				for(final KeyEntity keyEntity : valueEntity.getCSEntity().getKeyEntities()) {

					pathEndNodeIds.add(keyEntity.getNodeId());
				}

				final long csEntityNodeId = valueEntity.getCSEntity().getNodeId();

				if(!deltaState.equals(DeltaState.MODIFICATION)) {

					pathEndNodeIds.add(valueEntity.getNodeId());
				} else if(csEntityNodeId >= 0) {

					if(!modifiedPathEndNodesIdsFromCSEntityMap.containsKey(valueEntity.getCSEntity())) {

						modifiedPathEndNodesIdsFromCSEntityMap.put(valueEntity.getCSEntity(), new HashSet<Long>());
					}

					modifiedPathEndNodesIdsFromCSEntityMap.get(valueEntity.getCSEntity()).add(valueEntity.getNodeId());
				} else {

					pathEndNodeIds.add(valueEntity.getNodeId());
				}

				if(csEntityNodeId >= 0) {


					final Set<Long> pathEndNodeIdsFromCSEntity;

					if(pathEndNodesIdsFromCSEntityMap.containsKey(valueEntity.getCSEntity())) {

						pathEndNodeIdsFromCSEntity = pathEndNodesIdsFromCSEntityMap.get(valueEntity.getCSEntity());
					} else {

						pathEndNodeIdsFromCSEntity = new HashSet<>();
					}

					fetchEntityTypeNodes(graphDB, pathEndNodeIdsFromCSEntity, csEntityNodeId);
					// TODO: we can't do this here, or? - I think we'll probably mark too much
					determineNonMatchedSubGraphPathEndNodes(deltaState, graphDB, pathEndNodeIdsFromCSEntity, csEntityNodeId);

					pathEndNodesIdsFromCSEntityMap.put(valueEntity.getCSEntity(), pathEndNodeIdsFromCSEntity);
				}
			}
		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		final DeltaState finalDeltaState;

		if(!deltaState.equals(DeltaState.MODIFICATION)) {

			finalDeltaState = deltaState;
		} else {

			finalDeltaState = DeltaState.ExactMatch;
		}

		markPaths(finalDeltaState, graphDB, resourceURI, pathEndNodeIds);

		for(final Map.Entry<CSEntity, Set<Long>> pathEndNideIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			markPaths(finalDeltaState, graphDB, pathEndNideIdsFromCSEntityEntry.getKey().getNodeId(), pathEndNideIdsFromCSEntityEntry.getValue());
		}

		for(final Map.Entry<CSEntity, Set<Long>> modifiedPathEndNodeIdsFromCSEntityEntry : modifiedPathEndNodesIdsFromCSEntityMap.entrySet()) {

			markPaths(deltaState, graphDB, modifiedPathEndNodeIdsFromCSEntityEntry.getKey().getNodeId(), modifiedPathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}

	/**
	 * note: we could also include everything from a sub entity that was marked as deleted or added, since then the whole sub
	 * entity is affected to this state
	 *
	 * @param matchedSubGraphEntities
	 * @param deltaState
	 * @param graphDB
	 */
	public static void markSubGraphEntityPaths(final Collection<SubGraphEntity> matchedSubGraphEntities, final DeltaState deltaState,
			final GraphDatabaseService graphDB) {

		final Map<Long, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();

			// calc path end nodes
			for(final SubGraphEntity subGraphEntity : matchedSubGraphEntities) {
				
				try (final Transaction ignored = graphDB.beginTx()) {

					final Collection<String> leafNodes = getEntityLeafs(graphDB, subGraphEntity.getNodeId());

					if(leafNodes != null && !leafNodes.isEmpty()) {

						final Set<Long> pathEndNodeIds;

						if(pathEndNodesIdsFromCSEntityMap.containsKey(subGraphEntity.getCSEntity().getNodeId())) {

							pathEndNodeIds = pathEndNodesIdsFromCSEntityMap.get(subGraphEntity.getCSEntity().getNodeId());
						} else {

							pathEndNodeIds = new HashSet<>();
						}

						for(final String leafNode : leafNodes) {

							pathEndNodeIds.add(Long.valueOf(leafNode));
						}

						pathEndNodesIdsFromCSEntityMap.put(subGraphEntity.getCSEntity().getNodeId(), pathEndNodeIds);
					}
				} catch (final Exception e) {

					// TODO: log something

					e.printStackTrace();
				}
			}

		for(final Map.Entry<Long, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			markPaths(deltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey(), pathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}

	/**
	 *
	 * @param matchedSubGraphLeafEntities
	 * @param deltaState
	 * @param graphDB
	 */
	public static void markSubGraphLeafEntityPaths(final Collection<SubGraphLeafEntity> matchedSubGraphLeafEntities, final DeltaState deltaState,
			final GraphDatabaseService graphDB) {

		final Map<Long, Set<Long>> pathEndNodesIdsFromCSEntityMap = new HashMap<>();

		for(final SubGraphLeafEntity subGraphLeafEntity : matchedSubGraphLeafEntities) {

			if(!pathEndNodesIdsFromCSEntityMap.containsKey(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getNodeId())) {

				pathEndNodesIdsFromCSEntityMap.put(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getNodeId(), new HashSet<Long>());
			}

			pathEndNodesIdsFromCSEntityMap.get(subGraphLeafEntity.getSubGraphEntity().getCSEntity().getNodeId()).add(subGraphLeafEntity.getNodeId());
		}

		for(final Map.Entry<Long, Set<Long>> pathEndNodeIdsFromCSEntityEntry : pathEndNodesIdsFromCSEntityMap.entrySet()) {

			markPaths(deltaState, graphDB, pathEndNodeIdsFromCSEntityEntry.getKey(), pathEndNodeIdsFromCSEntityEntry.getValue());
		}
	}

	private static void determineNonMatchedSubGraphPathEndNodes(final DeltaState deltaState, final GraphDatabaseService graphDB,
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

	private static void markPaths(final DeltaState deltaState, final GraphDatabaseService graphDB, final String resourceURI,
			final Set<Long> pathEndNodeIds) {

		final Transaction tx = graphDB.beginTx();

		final Iterable<Path> paths = getResourcePaths(graphDB, resourceURI);

		markPaths(deltaState, pathEndNodeIds, paths);

		tx.success();
		tx.close();
	}

	private static void markPaths(final DeltaState deltaState, final GraphDatabaseService graphDB, final long nodeId, final Set<Long> pathEndNodeIds) {

		final Transaction tx = graphDB.beginTx();

		final Iterable<Path> paths = getEntityPaths(graphDB, nodeId);

		markPaths(deltaState, pathEndNodeIds, paths);

		tx.success();
	}

	private static void markPaths(final DeltaState deltaState, final Set<Long> pathEndNodeIds, final Iterable<Path> paths) {

		for (final Path path : paths) {

			final long pathEndNodeId = path.endNode().getId();

			if (pathEndNodeIds.contains(pathEndNodeId)) {

				// mark path
				for (final Relationship rel : path.relationships()) {

					if (!rel.hasProperty("DELTA_STATE")) {

						rel.setProperty("DELTA_STATE", deltaState.toString());
					}

					rel.setProperty("MATCHED", true);
				}

				for (final Node node : path.nodes()) {

					if (!node.hasProperty("DELTA_STATE")) {

						node.setProperty("DELTA_STATE", deltaState.toString());
					} else if(deltaState.equals(DeltaState.ExactMatch)) {


						final String deltaStateString = (String) node.getProperty("DELTA_STATE");
						final DeltaState currentDeltaState = DeltaState.getByName(deltaStateString);

						switch(currentDeltaState) {

							case ADDITION:
							case DELETION:
							case MODIFICATION:

								// modify delta state if a "higher" delta state was determined
								node.setProperty("DELTA_STATE", deltaState.toString());

								break;
						}
					}

					node.setProperty("MATCHED", true);
				}
			}
		}
	}

	public static Collection<CSEntity> getCSEntities(final GraphDatabaseService graphDB, final String resourceURI, final AttributePath commonAttributePath, final ContentSchema contentSchema) {

		final Transaction tx = graphDB.beginTx();

		final Node resourceNode = getResourceNode(graphDB, resourceURI);

		// determine CS entity nodes
		final ResourceIterable<Node> csEntityNodes = graphDB.traversalDescription().breadthFirst()
				.evaluator(Evaluators.toDepth(commonAttributePath.getAttributes().size())).evaluator(new EntityEvaluator(commonAttributePath.getAttributes()))
				.traverse(resourceNode).nodes();

		if(csEntityNodes == null) {

			return null;
		}

		final Map<Long, CSEntity> csEntities = new LinkedHashMap<>();

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
		tx.close();

		final Collection<CSEntity> csEntitiesCollection = csEntities.values();

		// determine cs entity order
		determineCSEntityOrder(csEntitiesCollection);

		return csEntitiesCollection;
	}

	public static Collection<SubGraphLeafEntity> getSubGraphLeafEntities(final Collection<SubGraphEntity> subGraphEntities, final GraphDatabaseService graphDB) {

		final Set<SubGraphLeafEntity> subGraphLeafEntities = new HashSet<>();

		for(final SubGraphEntity subGraphEntity : subGraphEntities) {

			final Map<String, String> subGraphLeafs = GraphDBUtil.getEntityLeafsWithValue(graphDB, subGraphEntity.getNodeId());

			if(subGraphLeafs == null || subGraphLeafs.isEmpty()) {

				continue;
			}

			for(final Map.Entry<String, String> subGraphLeafEntry : subGraphLeafs.entrySet()) {

				final SubGraphLeafEntity subGraphLeafEntity = new SubGraphLeafEntity(Long.valueOf(subGraphLeafEntry.getKey()), subGraphLeafEntry.getValue(), subGraphEntity);

				subGraphLeafEntities.add(subGraphLeafEntity);
			}
		}

		return subGraphLeafEntities;
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

						if (path.lastRelationship().hasProperty("MATCHED")) {

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

		try (final Transaction ignored = graphDB.beginTx()) {

			for(final CSEntity csEntity : csEntities) {

				final Node csEntityNode = graphDB.getNodeById(csEntity.getNodeId());
				final Iterable<Relationship> csEntityOutgoingRels = csEntityNode.getRelationships(Direction.OUTGOING);

				if(csEntityOutgoingRels == null || !csEntityOutgoingRels.iterator().hasNext()) {

					continue;
				}

				final List<Relationship> nonMatchedRels = new ArrayList<>();

				for(final Relationship csEntityOutgoingRel : csEntityOutgoingRels) {

					if(csEntityOutgoingRel.hasProperty("MATCHED")) {

						continue;
					}

					nonMatchedRels.add(csEntityOutgoingRel);
				}

				if(nonMatchedRels.isEmpty()) {

					// everything is already matched
					continue;
				}

				final String deltaStateString = (String) csEntityNode.getProperty("DELTA_STATE", null);
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

						System.out.println("rel id = '" + relId + "', value = '" + value +"'");
					}

					final long endNodeId = nonMatchedRel.getEndNode().getId();
					final int endNodeHierarchyLevel = (int) nonMatchedRel.getEndNode().getProperty("__HIERARCHY_LEVEL__");

					final SubGraphEntity subGraphEntity = new SubGraphEntity(endNodeId, predicate, deltaState, csEntity, finalOrder, endNodeHierarchyLevel);
					subgraphEntities.add(subGraphEntity);
				}
			}

		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
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

		final Node resourceNode = getResourceNode(graphDB, resourceURI);

		final Collection<ValueEntity> values = getFlatNodeValues(resourceNode, graphDB);

		tx.success();
		tx.close();

		return values;
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

			final GDMValueEntity valueEntity = new GDMValueEntity(valueNodeId, value, valueOrder, valueNodeType);

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

	private static String buildGetEntityHierarchyLevelNodesQuery(final long nodeId, final int hierarchyLevel) {

		// START n= node(2062) MATCH (n)-[r*]->(m) RETURN m;

		final StringBuilder sb = new StringBuilder();

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m)").append("\nWHERE m.__HIERARCHY_LEVEL__ = ").append(hierarchyLevel)
				.append("\nRETURN id(m) AS hierarchy_level_node");

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

		sb.append("START n=node(").append(nodeId).append(")\nMATCH (n)-[r*]->(m:`").append("__LEAF__").append("`)\nRETURN id(m) AS leaf_node, m.").append(GraphStatics.URI_PROPERTY).append(" AS leaf_uri, m.").append(GraphStatics.VALUE_PROPERTY).append(" AS leaf_value");

		return sb.toString();
	}

	private static String executeQueryWithSingleResult(final String query, final String resultVariableName, final GraphDatabaseService graphDB) {

		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result;
		String resultValue = null;

		try (final Transaction ignored = graphDB.beginTx()) {

			result = engine.execute(query);

			if(result != null) {

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

		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		return resultValue;
	}

	private static Collection<String> executeQueryWithMultipleResults(final String query, final String resultVariableName, final GraphDatabaseService graphDB) {

		final Set<String> resultSet = new HashSet<>();
		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result;

		try (final Transaction ignored = graphDB.beginTx()) {

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

		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		return resultSet;
	}

	private static Map<String, String> executeQueryWithMultipleResultsWithValues(final String query, final String resultVariableName, final String uriVariableName, final String valueVariableName, final GraphDatabaseService graphDB) {

		final Map<String, String> resultSet = new HashMap<>();
		final ExecutionEngine engine = new ExecutionEngine(graphDB);

		final ExecutionResult result;

		try (final Transaction ignored = graphDB.beginTx()) {

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

		} catch (final Exception e) {

			// TODO: log something

			e.printStackTrace();
		}

		return resultSet;
	}
}
