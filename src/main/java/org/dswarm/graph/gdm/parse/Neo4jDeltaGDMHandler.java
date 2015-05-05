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
package org.dswarm.graph.gdm.parse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Optional;
import com.hp.hpl.jena.vocabulary.RDF;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.parse.Neo4jHandler;

/**
 * TODO: re-factor usage of CommonHandler
 *
 * @author tgaengler
 */
public class Neo4jDeltaGDMHandler implements GDMHandler {

	private static final Logger LOG        = LoggerFactory.getLogger(Neo4jDeltaGDMHandler.class);
	public static final  int    DELTA_SIZE = 50000;
	public static final  int    DELTA_TIME = 30;

	private long totalTriples       = 0;
	private int addedNodes         = 0;
	private int addedLabels        = 0;
	private int addedRelationships = 0;
	private long sinceLastCommit    = 0;
	private int i                  = 0;
	private int literals           = 0;

	private long tick = System.currentTimeMillis();
	private final GraphDatabaseService database;
	private final Map<String, Node>    bnodes;
	private final Index<Relationship>  statementHashes;
	private final Index<Relationship>  statementUUIDs;
	private final Map<Long, Long>      nodeResourceMap;
	private final NamespaceIndex namespaceIndex;

	private Transaction tx;

	public Neo4jDeltaGDMHandler(final GraphDatabaseService database, final NamespaceIndex namespaceIndexArg) throws DMPGraphException {

		this.database = database;

		namespaceIndex = namespaceIndexArg;

		tx = database.beginTx();

		try {

			LOG.debug("start write TX");

			bnodes = new HashMap<>();
			statementHashes = database.index().forRelationships(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);
			statementUUIDs = database.index().forRelationships(GraphIndexStatics.STATEMENT_UUIDS_INDEX_NAME);
			nodeResourceMap = new HashMap<>();
		} catch (final Exception e) {

			tx.failure();
			tx.close();

			final String message = "couldn't load indices successfully";

			Neo4jDeltaGDMHandler.LOG.error(message, e);
			Neo4jDeltaGDMHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void handleStatement(final Statement st, final long resourceHash, final long index) throws DMPGraphException {

		// utilise r for the resource property

		i++;

		try {

			final org.dswarm.graph.json.Node subject = st.getSubject();

			final org.dswarm.graph.json.Predicate predicate = st.getPredicate();
			final String predicateName = predicate.getUri();
			final String prefixedPredicateURI = namespaceIndex.createPrefixedURI(predicateName);

			final org.dswarm.graph.json.Node object = st.getObject();

			final Long statementUUID = getUUID(st.getUUID());
			final Long order = st.getOrder();

			// Check index for subject
			// TODO: what should we do, if the subject is a resource type?
			Node subjectNode = determineNode(subject, false);

			if (subjectNode == null) {

				subjectNode = database.createNode();

				if (subject instanceof ResourceNode) {

					subjectNode = database.createNode(GraphProcessingStatics.RESOURCE_LABEL);

					final String subjectURI = ((ResourceNode) subject).getUri();
					final String prefixedSubjectURI = namespaceIndex.createPrefixedURI(subjectURI);

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, prefixedSubjectURI);
				} else {

					// subject is a blank node

					subjectNode = database.createNode(GraphProcessingStatics.BNODE_LABEL);

					// note: can I expect an id here?
					bnodes.put("" + subject.getId(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				}

				addedNodes++;
			}

			if (object instanceof LiteralNode) {

				literals++;

				final LiteralNode literal = (LiteralNode) object;
				final String value = literal.getValue();
				final Node objectNode = database.createNode(GraphProcessingStatics.LEAF_LABEL, GraphProcessingStatics.LITERAL_LABEL);
				objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value);
				objectNode.setProperty(GraphProcessingStatics.LEAF_IDENTIFIER, true);

				final long finalResourceHash = addResourceProperty(subjectNode, subject, objectNode, resourceHash);

				addedNodes++;

				addRelationship(subjectNode, prefixedPredicateURI, objectNode, Optional.of(finalResourceHash), subject, resourceHash, statementUUID, order,
						index,
						subject.getType(), object.getType());
			} else { // must be Resource
				// Make sure object exists

				boolean isType = false;

				final String prefixedObjectURI;

				// add Label if this is a type entry
				if (predicateName.equals(RDF.type.getURI())) {

					final String objectURI = ((ResourceNode) object).getUri();
					prefixedObjectURI = namespaceIndex.createPrefixedURI(objectURI);

					addLabel(subjectNode, prefixedObjectURI);

					isType = true;
				} else {

					prefixedObjectURI = null;
				}

				// Check index for object
				Node objectNode = determineNode(object, isType);
				Optional<Long> optionalResourceHash = Optional.absent();

				if (objectNode == null) {

					if (object instanceof ResourceNode) {

						// object is a resource node

						objectNode = database.createNode(GraphProcessingStatics.LEAF_LABEL, GraphProcessingStatics.RESOURCE_LABEL);
						objectNode.setProperty(GraphProcessingStatics.LEAF_IDENTIFIER, true);

						objectNode.setProperty(GraphStatics.URI_PROPERTY, prefixedObjectURI);

						if (isType) {

							addLabel(objectNode, NodeType.TypeResource.toString());
							addLabel(objectNode, namespaceIndex.getRDFCLASSPrefixedURI());
						}
					} else {

						// object is a blank node

						objectNode = database.createNode(GraphProcessingStatics.BNODE_LABEL);

						bnodes.put("" + object.getId(), objectNode);

						if (!isType) {

							optionalResourceHash = Optional.of(addResourceProperty(subjectNode, subject, objectNode, resourceHash));
						} else {

							addLabel(objectNode, NodeType.TypeBNode.toString());
							addLabel(objectNode, namespaceIndex.getRDFCLASSPrefixedURI());
						}
					}

					addedNodes++;
				}

				addRelationship(subjectNode, prefixedPredicateURI, objectNode, optionalResourceHash, subject, resourceHash, statementUUID, order, index,
						subject.getType(),
						object.getType());
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= DELTA_SIZE || timeDelta >= DELTA_TIME) { // Commit every 50k operations or every 30 seconds

				tx.success();
				tx.close();
				tx = database.beginTx();

				sinceLastCommit = totalTriples;

				final double duration = (double) nodeDelta / timeDelta;
				LOG.debug("{} triples @ ~{} triples/second.", totalTriples, duration);

				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			final String message = "couldn't finish write TX successfully";

			LOG.error(message, e);

			tx.failure();
			tx.close();

			throw new DMPGraphException(message);
		}
	}

	@Override public NamespaceIndex getNamespaceIndex() {

		return namespaceIndex;
	}

	@Override public Neo4jHandler getHandler() {

		// nothing TODO here ...

		return null;
	}

	@Override public GraphDatabaseService getDatabase() {

		return database;
	}

	public void closeTransaction() {

		LOG.debug("close write TX finally");

		tx.success();
		tx.close();
	}

	@Override
	public long getCountedStatements() {

		return totalTriples;
	}

	@Override
	public int getNodesAdded() {

		return addedNodes;
	}

	@Override
	public int getRelationshipsAdded() {

		return addedRelationships;
	}

	@Override
	public int getCountedLiterals() {

		return literals;
	}

	private void addLabel(final Node node, final String labelString) {

		final Label label = DynamicLabel.label(labelString);
		boolean hit = false;
		final Iterable<Label> labels = node.getLabels();

		for (final Label lbl : labels) {

			if (label.equals(lbl)) {

				hit = true;
				break;
			}
		}

		if (!hit) {

			node.addLabel(label);
			addedLabels++;
		}
	}

	private Relationship addRelationship(final Node subjectNode, final String predicateName, final Node objectNode,
			final Optional<Long> optionalResourceHash,
			final org.dswarm.graph.json.Node subject, final long resourceHash, final Long statementUUID, final Long order, final long index,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final StringBuilder sb = new StringBuilder();

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);
		final String objectIdentifier = getIdentifier(objectNode, objectNodeType);

		sb.append(subjectNodeType.toString()).append(":").append(subjectIdentifier).append(" ").append(predicateName).append(" ")
				.append(objectNodeType.toString()).append(":").append(objectIdentifier).append(" ");

		final long hash = HashUtils.generateHash(sb.toString());

		final Relationship rel;

		IndexHits<Relationship> hits = statementHashes.get(GraphStatics.HASH, hash);

		if (hits == null || !hits.hasNext()) {

			final RelationshipType relType = DynamicRelationshipType.withName(predicateName);
			rel = subjectNode.createRelationshipTo(objectNode, relType);

			final Long finalStatementUUID;

			if (statementUUID == null) {

				finalStatementUUID = HashUtils.generateHash(UUID.randomUUID().toString());
			} else {

				finalStatementUUID = statementUUID;
			}

			rel.setProperty(GraphStatics.UUID_PROPERTY, finalStatementUUID);

			if (order != null) {

				rel.setProperty(GraphStatics.ORDER_PROPERTY, order);
			}

			rel.setProperty(GraphStatics.INDEX_PROPERTY, index);

			statementHashes.add(rel, GraphStatics.HASH, hash);
			statementUUIDs.add(rel, GraphStatics.UUID, finalStatementUUID);

			addedRelationships++;

			addResourceProperty(subjectNode, subject, rel, optionalResourceHash, resourceHash);
		} else {

			rel = hits.next();
		}

		if (hits != null) {

			hits.close();
		}

		return rel;
	}

	private Node determineNode(final org.dswarm.graph.json.Node resource, final boolean isType) throws DMPGraphException {

		final Node node;

		if (resource instanceof ResourceNode) {

			// resource node

			final String resourceURI = ((ResourceNode) resource).getUri();
			final String prefixedResourceURI = namespaceIndex.createPrefixedURI(resourceURI);

			if (!isType) {

				return database.findNode(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.URI_PROPERTY, prefixedResourceURI);
			} else {

				return database.findNode(GraphProcessingStatics.RESOURCE_TYPE_LABEL, GraphStatics.URI_PROPERTY, prefixedResourceURI);
			}
		}

		if (resource instanceof LiteralNode) {

			// literal node - should never be the case

			return null;
		}

		// resource must be a blank node

		node = bnodes.get("" + resource.getId());

		return node;
	}

	private long addResourceProperty(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Node objectNode,
			final long resourceHash) throws DMPGraphException {

		final long finalResourceHash = determineResourceHash(subjectNode, subject, resourceHash);

		objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, finalResourceHash);

		return finalResourceHash;
	}

	private long addResourceProperty(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Relationship rel,
			final Optional<Long> optionalResourceHash, final long resourceHash) throws DMPGraphException {

		final long finalResourceHash;

		if (optionalResourceHash.isPresent()) {

			finalResourceHash = optionalResourceHash.get();
		} else {

			finalResourceHash = determineResourceHash(subjectNode, subject, resourceHash);
		}

		rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalResourceHash);

		return finalResourceHash;
	}

	private long determineResourceHash(final Node subjectNode, final org.dswarm.graph.json.Node subject, final long resourceHash)
			throws DMPGraphException {

		final long nodeId = subjectNode.getId();

		final long finalResourceHash;

		if (nodeResourceMap.containsKey(nodeId)) {

			finalResourceHash = nodeResourceMap.get(nodeId);
		} else {

			if (subject instanceof ResourceNode) {

				final String subjectURI = ((ResourceNode) subject).getUri();
				final String prefixedSubjectURI = namespaceIndex.createPrefixedURI(subjectURI);
				finalResourceHash = HashUtils.generateHash(prefixedSubjectURI);
			} else {

				finalResourceHash = resourceHash;
			}

			nodeResourceMap.put(nodeId, finalResourceHash);
		}

		return finalResourceHash;
	}

	private String getIdentifier(final Node node, final org.dswarm.graph.json.NodeType nodeType) {

		final String identifier;

		switch (nodeType) {

			case Resource:

				identifier = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);

				break;
			case BNode:

				identifier = "" + node.getId();

				break;
			case Literal:

				identifier = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);

				break;
			default:

				identifier = null;

				break;
		}

		return identifier;
	}

	private Long getUUID(final String uuid) {

		if(uuid == null) {

			return null;
		}

		try {

			return Long.valueOf(uuid);
		} catch (final NumberFormatException e) {

			return HashUtils.generateHash(uuid);
		}
	}
}
