package org.dswarm.graph.gdm.parse;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
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

import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public abstract class Neo4jBaseGDMHandler implements GDMHandler {

	private static final Logger				LOG					= LoggerFactory.getLogger(Neo4jBaseGDMHandler.class);

	protected int							totalTriples		= 0;
	protected int							addedNodes			= 0;
	protected int							addedLabels			= 0;
	protected int							addedRelationships	= 0;
	protected int							sinceLastCommit		= 0;
	protected int							i					= 0;
	protected int							literals			= 0;

	protected long							tick				= System.currentTimeMillis();
	protected final GraphDatabaseService	database;
	protected final Index<Node>				resources;
	protected final Index<Node>				resourcesWProvenance;
	protected final Index<Node>				resourceTypes;
	protected final Index<Node>				values;
	protected final Map<String, Node>		bnodes;
	protected final Index<Relationship>		statementHashes;
	protected final Map<Long, String>		nodeResourceMap;

	protected Transaction					tx;

	public Neo4jBaseGDMHandler(final GraphDatabaseService database) {

		this.database = database;
		tx = database.beginTx();

		LOG.debug("start write TX");

		resources = database.index().forNodes("resources");
		resourcesWProvenance = database.index().forNodes("resources_w_provenance");
		resourceTypes = database.index().forNodes("resource_types");
		values = database.index().forNodes("values");
		bnodes = new HashMap<>();
		statementHashes = database.index().forRelationships("statement_hashes");
		nodeResourceMap = new HashMap<>();
	}

	@Override
	public void handleStatement(final Statement st, final Resource r, final long index) {

		// utilise r for the resource property

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

		try {

			final org.dswarm.graph.json.Node subject = st.getSubject();

			final org.dswarm.graph.json.Predicate predicate = st.getPredicate();
			final String predicateName = predicate.getUri();

			final org.dswarm.graph.json.Node object = st.getObject();

			final String statementUUID = st.getUUID();
			final Long order = st.getOrder();

			// Check index for subject
			// TODO: what should we do, if the subject is a resource type?
			Node subjectNode = determineNode(subject, false);

			if (subjectNode == null) {

				subjectNode = database.createNode();

				if (subject instanceof ResourceNode) {

					// subject is a resource node

					final String subjectURI = ((ResourceNode) subject).getUri();

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subjectURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

					final String provenanceURI = ((ResourceNode) subject).getProvenance();

					handleSubjectProvenance(subjectNode, subjectURI, provenanceURI);

					resources.add(subjectNode, GraphStatics.URI, subjectURI);
				} else {

					// subject is a blank node

					// note: can I expect an id here?
					bnodes.put("" + subject.getId(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				}

				addedNodes++;
			}

			if (object instanceof LiteralNode) {

				handleLiteral(r, index, subject, predicateName, object, statementUUID, order, subjectNode);
			} else { // must be Resource
						// Make sure object exists

				boolean isType = false;

				// add Label if this is a type entry
				if (predicateName.equals(RDF.type.getURI())) {

					addLabel(subjectNode, ((ResourceNode) object).getUri());

					isType = true;
				}

				// Check index for object
				Node objectNode = determineNode(object, isType);
				String resourceUri = null;

				if (objectNode == null) {

					objectNode = database.createNode();

					if (object instanceof ResourceNode) {

						// object is a resource node

						final String objectURI = ((ResourceNode) object).getUri();
						final String provenanceURI = ((ResourceNode) object).getProvenance();

						objectNode.setProperty(GraphStatics.URI_PROPERTY, objectURI);

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

							handleObjectProvenance(objectNode, provenanceURI);
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
							addLabel(objectNode, RDFS.Class.getURI());

							resourceTypes.add(objectNode, GraphStatics.URI, objectURI);
						}

						resources.add(objectNode, GraphStatics.URI, objectURI);

						addObjectToResourceWProvenanceIndex(objectNode, objectURI, provenanceURI);
					} else {

						resourceUri = handleBNode(r, subject, object, subjectNode, isType, objectNode);
					}

					addedNodes++;
				}

				final String hash = generateStatementHash(subjectNode, predicateName, objectNode, subject.getType(), object.getType());

				final Relationship rel = getStatement(hash);
				if (rel == null) {

					addRelationship(subjectNode, predicateName, objectNode, resourceUri, subject, r, statementUUID, order, index, hash);
				}
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 50000 || timeDelta >= 30) { // Commit every 50k operations or every 30 seconds

				tx.success();
				tx.close();
				tx = database.beginTx();

				sinceLastCommit = totalTriples;

				LOG.debug(totalTriples + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");

				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			LOG.error("couldn't finished write TX successfully", e);

			tx.failure();
			tx.close();
			LOG.debug("close a write TX");

			tx = database.beginTx();

			LOG.debug("start another write TX");

		} finally {

			// ???
		}
	}

	protected abstract void addObjectToResourceWProvenanceIndex(final Node node, final String URI, final String provenanceURI);

	protected abstract void handleObjectProvenance(Node node, String provenanceURI);

	protected abstract void handleSubjectProvenance(final Node node, String URI, final String provenanceURI);

	@Override
	public void closeTransaction() {

		LOG.debug("close write TX finally");

		tx.success();
		tx.close();
	}

	public int getCountedStatements() {

		return totalTriples;
	}

	public int getNodesAdded() {

		return addedNodes;
	}

	public int getRelationShipsAdded() {

		return addedRelationships;
	}

	public int getCountedLiterals() {

		return literals;
	}

	protected String handleBNode(final Resource r, final org.dswarm.graph.json.Node subject, final org.dswarm.graph.json.Node object,
			final Node subjectNode, final boolean isType, final Node objectNode) {

		String resourceUri = null;

		// object is a blank node

		bnodes.put("" + object.getId(), objectNode);

		if (!isType) {

			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
			resourceUri = addResourceProperty(subjectNode, subject, objectNode, r);
		} else {

			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeBNode.toString());
			addLabel(objectNode, RDFS.Class.getURI());
		}
		return resourceUri;
	}

	protected void handleLiteral(final Resource r, final long index, final org.dswarm.graph.json.Node subject, final String predicateName,
			final org.dswarm.graph.json.Node object, final String statementUUID, final Long order, final Node subjectNode) throws DMPGraphException {

		final LiteralNode literal = (LiteralNode) object;
		final String value = literal.getValue();

		final String hash = generateStatementHash(subjectNode, predicateName, value, subject.getType(), object.getType());

		final Relationship rel = getStatement(hash);

		if (rel == null) {

			literals++;

			final Node objectNode = database.createNode();
			objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value);
			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
			values.add(objectNode, GraphStatics.VALUE, value);

			final String resourceUri = addResourceProperty(subjectNode, subject, objectNode, r);

			addedNodes++;

			addRelationship(subjectNode, predicateName, objectNode, resourceUri, subject, r, statementUUID, order, index, hash);
		}
	}

	protected void addLabel(final Node node, final String labelString) {

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

	protected Relationship prepareRelationship(final Node subjectNode, final String predicateName, final Node objectNode, final String statementUUID,
			final Long order, final long index) {

		final RelationshipType relType = DynamicRelationshipType.withName(predicateName);
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (order != null) {

			rel.setProperty(GraphStatics.ORDER_PROPERTY, order);
		}

		rel.setProperty(GraphStatics.INDEX_PROPERTY, index);

		return rel;
	}

	protected abstract void addStatementToIndex(final Relationship rel, final String statementUUID);

	protected Relationship addRelationship(final Node subjectNode, final String predicateName, final Node objectNode, final String resourceUri,
			final org.dswarm.graph.json.Node subject, final Resource resource, final String statementUUID, final Long order, final long index,
			final String hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (statementUUID == null) {

			finalStatementUUID = UUID.randomUUID().toString();
		} else {

			finalStatementUUID = statementUUID;
		}

		final Relationship rel = prepareRelationship(subjectNode, predicateName, objectNode, finalStatementUUID, order, index);

		statementHashes.add(rel, GraphStatics.HASH, hash);
		addStatementToIndex(rel, finalStatementUUID);

		addedRelationships++;

		addResourceProperty(subjectNode, subject, rel, resourceUri, resource);

		return rel;
	}

	protected Relationship getStatement(final String hash) throws DMPGraphException {

		IndexHits<Relationship> hits = statementHashes.get(GraphStatics.HASH, hash);

		if (hits != null && hits.hasNext()) {

			return hits.next();
		}

		return null;
	}

	protected String generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);
		final String objectIdentifier = getIdentifier(objectNode, objectNodeType);

		return generateStatementHash(predicateName, subjectNodeType, objectNodeType, subjectIdentifier, objectIdentifier);
	}

	private String generateStatementHash(final Node subjectNode, final String predicateName, final String objectValue,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);

		return generateStatementHash(predicateName, subjectNodeType, objectNodeType, subjectIdentifier, objectValue);
	}

	private String generateStatementHash(final String predicateName, final org.dswarm.graph.json.NodeType subjectNodeType,
			final org.dswarm.graph.json.NodeType objectNodeType, final String subjectIdentifier, final String objectIdentifier)
			throws DMPGraphException {

		final StringBuffer sb = new StringBuffer();

		sb.append(subjectNodeType.toString()).append(":").append(subjectIdentifier).append(" ").append(predicateName).append(" ")
				.append(objectNodeType.toString()).append(":").append(objectIdentifier).append(" ");

		MessageDigest messageDigest = null;

		try {

			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e) {

			throw new DMPGraphException("couldn't instantiate hash algo");
		}
		messageDigest.update(sb.toString().getBytes());

		return new String(messageDigest.digest());
	}

	private Node determineNode(final org.dswarm.graph.json.Node resource, final boolean isType) {

		final Node node;

		if (resource instanceof ResourceNode) {

			// resource node

			final IndexHits<Node> hits;

			if (!isType) {

				if (((ResourceNode) resource).getProvenance() == null) {

					hits = getResourceNodeHits((ResourceNode) resource);
				} else {

					hits = resourcesWProvenance.get(GraphStatics.URI_W_PROVENANCE,
							((ResourceNode) resource).getUri() + ((ResourceNode) resource).getProvenance());
				}
			} else {

				hits = resourceTypes.get(GraphStatics.URI, ((ResourceNode) resource).getUri());
			}

			if (hits != null && hits.hasNext()) {

				// node exists

				node = hits.next();

				return node;
			}

			return null;
		}

		if (resource instanceof LiteralNode) {

			// literal node - should never be the case

			return null;
		}

		// resource must be a blank node

		node = bnodes.get("" + resource.getId());

		return node;
	}

	protected abstract IndexHits<Node> getResourceNodeHits(final ResourceNode resource);

	protected String addResourceProperty(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Node objectNode,
			final Resource resource) {

		final String resourceUri = determineResourceUri(subjectNode, subject, resource);

		if (resourceUri == null) {

			return null;
		}

		objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, resourceUri);

		return resourceUri;
	}

	protected String addResourceProperty(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Relationship rel,
			final String resourceUri, final Resource resource) {

		final String finalResourceUri;

		if (resourceUri != null) {

			finalResourceUri = resourceUri;
		} else {

			finalResourceUri = determineResourceUri(subjectNode, subject, resource);
		}

		rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalResourceUri);

		return finalResourceUri;
	}

	protected String determineResourceUri(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Resource resource) {

		final Long nodeId = subjectNode.getId();

		final String resourceUri;

		if (nodeResourceMap.containsKey(nodeId)) {

			resourceUri = nodeResourceMap.get(nodeId);
		} else {

			if (subject instanceof ResourceNode) {

				resourceUri = ((ResourceNode) subject).getUri();
			} else {

				resourceUri = resource.getUri();
			}

			nodeResourceMap.put(nodeId, resourceUri);
		}

		return resourceUri;
	}

	private String getIdentifier(final Node node, final org.dswarm.graph.json.NodeType nodeType) {

		final String identifier;

		switch (nodeType) {

			case Resource:

				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				final String provenance = (String) node.getProperty(GraphStatics.PROVENANCE_PROPERTY, null);

				if (provenance == null) {

					identifier = uri;
				} else {

					identifier = uri + provenance;
				}

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
}
