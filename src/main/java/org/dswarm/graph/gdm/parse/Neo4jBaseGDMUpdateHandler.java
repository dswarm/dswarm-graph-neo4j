package org.dswarm.graph.gdm.parse;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.gdm.read.PropertyGraphGDMReader;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.Range;
import org.dswarm.graph.versioning.VersioningStatics;
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
public abstract class Neo4jBaseGDMUpdateHandler implements GDMUpdateHandler {

	private static final Logger				LOG							= LoggerFactory.getLogger(Neo4jBaseGDMUpdateHandler.class);

	protected int							totalTriples				= 0;
	protected int							addedNodes					= 0;
	protected int							addedLabels					= 0;
	protected int							addedRelationships			= 0;
	protected int							sinceLastCommit				= 0;
	protected int							i							= 0;
	protected int							literals					= 0;

	protected long							tick						= System.currentTimeMillis();
	protected final GraphDatabaseService	database;
	protected final Index<Node>				resources;
	protected final Index<Node>				resourcesWProvenance;
	protected final Index<Node>				resourceTypes;
	protected final Index<Node>				values;
	protected final Map<String, Node>		bnodes;
	protected final Index<Relationship>		statementHashes;
	protected final Map<Long, String>		nodeResourceMap;

	protected Transaction					tx;

	protected String						resourceUri;

	private Range							range;

	protected boolean						latestVersionInitialized	= false;

	protected int							latestVersion;

	protected final PropertyGraphGDMReader	propertyGraphGDMReader		= new PropertyGraphGDMReader();

	public Neo4jBaseGDMUpdateHandler(final GraphDatabaseService database) throws DMPGraphException {

		this.database = database;

		tx = database.beginTx();

		try {

			LOG.debug("start write TX");

			resources = database.index().forNodes("resources");
			resourcesWProvenance = database.index().forNodes("resources_w_provenance");
			resourceTypes = database.index().forNodes("resource_types");
			values = database.index().forNodes("values");
			bnodes = new HashMap<>();
			statementHashes = database.index().forRelationships("statement_hashes");
			nodeResourceMap = new HashMap<>();
		}catch (final Exception e) {

			tx.failure();
			tx.close();

			final String message = "couldn't load indices successfully";

			Neo4jBaseGDMUpdateHandler.LOG.error(message, e);
			Neo4jBaseGDMUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	protected void init() {

		latestVersion = retrieveLatestVersion() + 1;
		range = Range.range(latestVersion);
	}

	@Override
	public void setResourceUri(final String resourceUriArg) {

		resourceUri = resourceUriArg;
	}

	@Override
	public void handleStatement(final Statement st, final Resource r, final long index) throws DMPGraphException {
		// utilise r for the resource property

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

		try {

			final org.dswarm.graph.json.Node subject = st.getSubject();

			final org.dswarm.graph.json.Predicate predicate = st.getPredicate();
			final String predicateName = predicate.getUri();

			final org.dswarm.graph.json.Node object = st.getObject();

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

					if (resourceUri != null && resourceUri.equals(subjectURI)) {

						setLatestVersion(provenanceURI);
					}

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

				handleLiteral(r, index, st, subjectNode);
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

					addRelationship(subjectNode, objectNode, resourceUri, r, st, index, hash);
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

			final String message = "couldn't finish write TX successfully";

			LOG.error(message, e);

			tx.failure();
			tx.close();

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void handleStatement(final String stmtUUID, final Resource resource, final long index, final long order) throws DMPGraphException {

		try {

			final Relationship rel = getRelationship(stmtUUID);
			final Node subject = rel.getStartNode();
			final Node object = rel.getEndNode();
			final Statement stmt = propertyGraphGDMReader.readStatement(rel);
			addBNode(stmt.getSubject(), subject);
			addBNode(stmt.getObject(), object);

			// reset stmt uuid, so that a new stmt uuid will be assigned when relationship will be added
			stmt.setUUID(null);
			// set actual order of the stmt
			stmt.setOrder(order);
			final String predicate = stmt.getPredicate().getUri();

			// TODO: shall we include some more qualified attributes into hash generation, e.g., index, valid from, or will the
			// index
			// be update with the new stmt (?)
			final String hash = generateStatementHash(subject, predicate, object, stmt.getSubject().getType(), stmt.getObject().getType());

			addRelationship(subject, object, resource.getUri(), resource, stmt, index, hash);

			totalTriples++;
		} catch (final DMPGraphException e) {

			throw e;
		} catch (final Exception e) {

			final String message = "couldn't handle statement successfully";

			tx.failure();
			tx.close();

			Neo4jBaseGDMUpdateHandler.LOG.error(message, e);
			Neo4jBaseGDMUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void deprecateStatement(long index) {

		throw new NotImplementedException();
	}

	@Override
	public org.dswarm.graph.json.Node deprecateStatement(final String uuid) throws DMPGraphException {

		try {

			final Relationship rel = getRelationship(uuid);

			rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, latestVersion);

			final org.dswarm.graph.json.Node subjectGDMNode = propertyGraphGDMReader.readObject(rel.getStartNode());
			final org.dswarm.graph.json.Node objectGDMNode = propertyGraphGDMReader.readObject(rel.getEndNode());
			addBNode(subjectGDMNode, rel.getStartNode());
			addBNode(objectGDMNode, rel.getEndNode());

			return subjectGDMNode;
		} catch (final DMPGraphException e) {

			throw e;
		} catch (final Exception e) {

			final String message = "couldn't deprecate statement successfully";

			tx.failure();
			tx.close();

			Neo4jBaseGDMUpdateHandler.LOG.error(message, e);
			Neo4jBaseGDMUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	protected abstract void addObjectToResourceWProvenanceIndex(final Node node, final String URI, final String provenanceURI);

	protected abstract void handleObjectProvenance(Node node, String provenanceURI);

	protected abstract void handleSubjectProvenance(final Node node, String URI, final String provenanceURI);

	protected abstract int retrieveLatestVersion();

	@Override
	public int getLatestVersion() {

		return latestVersion;
	}

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

	protected void setLatestVersion(final String provenanceURI) throws DMPGraphException {

		if (!latestVersionInitialized) {

			if (provenanceURI == null) {

				return;
			}

			Node dataModelNode = determineNode(new ResourceNode(provenanceURI), false);

			if (dataModelNode != null) {

				latestVersionInitialized = true;

				return;
			}

			dataModelNode = database.createNode();
			addLabel(dataModelNode, VersioningStatics.DATA_MODEL_TYPE);
			dataModelNode.setProperty(GraphStatics.URI_PROPERTY, provenanceURI);
			dataModelNode.setProperty(GraphStatics.PROVENANCE_PROPERTY, provenanceURI);
			dataModelNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
			dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, range.from());

			Node dataModelTypeNode = determineNode(new ResourceNode(VersioningStatics.DATA_MODEL_TYPE), true);

			if (dataModelTypeNode == null) {

				dataModelTypeNode = database.createNode();
				addLabel(dataModelTypeNode, RDFS.Class.getURI());
				dataModelTypeNode.setProperty(GraphStatics.URI_PROPERTY, VersioningStatics.DATA_MODEL_TYPE);
				dataModelTypeNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
			}

			final String hash = generateStatementHash(dataModelNode, RDF.type.getURI(), dataModelTypeNode, org.dswarm.graph.json.NodeType.Resource,
					org.dswarm.graph.json.NodeType.Resource);

			Relationship rel = getStatement(hash);

			if (rel == null) {

				final RelationshipType relType = DynamicRelationshipType.withName(RDF.type.getURI());
				rel = dataModelNode.createRelationshipTo(dataModelTypeNode, relType);
				rel.setProperty(GraphStatics.INDEX_PROPERTY, 0);
				rel.setProperty(GraphStatics.PROVENANCE_PROPERTY, provenanceURI);

				final String uuid = UUID.randomUUID().toString();

				rel.setProperty(GraphStatics.UUID_PROPERTY, uuid);

				statementHashes.add(rel, GraphStatics.HASH, hash);
				addStatementToIndex(rel, uuid);
			}

			latestVersionInitialized = true;
		}
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

	protected void handleLiteral(final Resource r, final long index, final Statement statement, final Node subjectNode) throws DMPGraphException {

		final LiteralNode literal = (LiteralNode) statement.getObject();
		final String value = literal.getValue();

		final String hash = generateStatementHash(subjectNode, statement.getPredicate().getUri(), value, statement.getSubject().getType(), statement
				.getObject().getType());

		final Relationship rel = getStatement(hash);

		if (rel == null) {

			literals++;

			final Node objectNode = database.createNode();
			objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value);
			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
			values.add(objectNode, GraphStatics.VALUE, value);

			final String resourceUri = addResourceProperty(subjectNode, statement.getSubject(), objectNode, r);

			addedNodes++;

			addRelationship(subjectNode, objectNode, resourceUri, r, statement, index, hash);
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

	protected Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index) {

		final RelationshipType relType = DynamicRelationshipType.withName(statement.getPredicate().getUri());
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (statement.getOrder() != null) {

			rel.setProperty(GraphStatics.ORDER_PROPERTY, statement.getOrder());
		}

		rel.setProperty(GraphStatics.INDEX_PROPERTY, index);
		rel.setProperty(VersioningStatics.VALID_FROM_PROPERTY, range.from());
		rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, range.to());

		if (statement.getEvidence() != null) {

			rel.setProperty(GraphStatics.EVIDENCE_PROPERTY, statement.getEvidence());
		}

		return rel;
	}

	protected abstract void addStatementToIndex(final Relationship rel, final String statementUUID);

	protected Relationship addRelationship(final Node subjectNode, final Node objectNode, final String resourceUri, final Resource resource,
			final Statement statement, final long index, final String hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (statement.getUUID() == null) {

			finalStatementUUID = UUID.randomUUID().toString();
		} else {

			finalStatementUUID = statement.getUUID();
		}

		final Relationship rel = prepareRelationship(subjectNode, objectNode, finalStatementUUID, statement, index);

		statementHashes.add(rel, GraphStatics.HASH, hash);
		addStatementToIndex(rel, finalStatementUUID);

		addedRelationships++;

		addResourceProperty(subjectNode, statement.getSubject(), rel, resourceUri, resource);

		return rel;
	}

	protected Relationship getStatement(final String hash) throws DMPGraphException {

		IndexHits<Relationship> hits = statementHashes.get(GraphStatics.HASH, hash);

		if (hits != null && hits.hasNext()) {

			final Relationship rel = hits.next();

			hits.close();

			return rel;
		}

		if (hits != null) {

			hits.close();
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

				hits.close();

				return node;
			}

			if (hits != null) {

				hits.close();
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

	protected abstract Relationship getRelationship(final String uuid);

	private void addBNode(final org.dswarm.graph.json.Node gdmNode, final Node node) {

		switch (gdmNode.getType()) {

			case BNode:

				bnodes.put("" + gdmNode.getId(), node);

				break;
		}
	}
}
