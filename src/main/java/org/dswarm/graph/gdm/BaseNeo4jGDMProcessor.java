package org.dswarm.graph.gdm;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

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
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
public abstract class BaseNeo4jGDMProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(BaseNeo4jGDMProcessor.class);

	protected int addedLabels = 0;

	protected final GraphDatabaseService database;
	protected final Index<Node>          resources;
	protected final Index<Node>          resourcesWDataModel;
	protected final Index<Node>          resourceTypes;
	protected final Index<Node>          values;
	protected final Map<String, Node>    bnodes;
	protected final Index<Relationship>  statementHashes;
	protected final Map<Long, String>    nodeResourceMap;

	protected Transaction tx;

	boolean txIsClosed = false;

	public BaseNeo4jGDMProcessor(final GraphDatabaseService database) throws DMPGraphException {

		this.database = database;
		beginTx();

		try {

			LOG.debug("start write TX");

			resources = database.index().forNodes("resources");
			resourcesWDataModel = database.index().forNodes("resources_w_data_model");
			resourceTypes = database.index().forNodes("resource_types");
			values = database.index().forNodes("values");
			bnodes = new HashMap<>();
			statementHashes = database.index().forRelationships("statement_hashes");
			nodeResourceMap = new HashMap<>();

		} catch (final Exception e) {

			tx.failure();
			tx.close();
			txIsClosed = true;

			final String message = "couldn't load indices successfully";

			BaseNeo4jGDMProcessor.LOG.error(message, e);
			BaseNeo4jGDMProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	public GraphDatabaseService getDatabase() {

		return database;
	}

	public Index<Node> getResourcesIndex() {

		return resources;
	}

	public Index<Node> getResourcesWDataModelIndex() {

		return resourcesWDataModel;
	}

	public Map<String, Node> getBNodesIndex() {

		return bnodes;
	}

	public Index<Node> getResourceTypesIndex() {

		return resourceTypes;
	}

	public Index<Node> getValueIndex() {

		return values;
	}

	public Index<Relationship> getStatementIndex() {

		return statementHashes;
	}

	public void beginTx() {

		tx = database.beginTx();
	}

	public void renewTx() {

		tx.success();
		tx.close();
		txIsClosed = true;
		tx = database.beginTx();
		txIsClosed = false;
	}

	public void failTx() {

		tx.failure();
		tx.close();
		txIsClosed = true;
	}

	public void succeedTx() {

		tx.success();
		tx.close();
		txIsClosed = true;
	}

	public void ensureRunningTx() {

		if(txIsClosed()) {

			beginTx();
		}
	}

	public boolean txIsClosed() {

		return txIsClosed;
	}

	public Node determineNode(final org.dswarm.graph.json.Node resource, final boolean isType) {

		final Node node;

		if (resource instanceof ResourceNode) {

			// resource node

			final IndexHits<Node> hits;

			if (!isType) {

				if (((ResourceNode) resource).getDataModel() == null) {

					hits = getResourceNodeHits((ResourceNode) resource);
				} else {

					hits = resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL,
							((ResourceNode) resource).getUri() + ((ResourceNode) resource).getDataModel());
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

			if(hits != null) {

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

	public String determineResourceUri(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Resource resource) {

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

	public void addLabel(final Node node, final String labelString) {

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

	public String generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);
		final String objectIdentifier = getIdentifier(objectNode, objectNodeType);

		return generateStatementHash(predicateName, subjectNodeType, objectNodeType, subjectIdentifier, objectIdentifier);
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final String objectValue,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);

		return generateStatementHash(predicateName, subjectNodeType, objectNodeType, subjectIdentifier, objectValue);
	}

	public Relationship getStatement(final String hash) throws DMPGraphException {

		IndexHits<Relationship> hits = statementHashes.get(GraphStatics.HASH, hash);

		if (hits != null && hits.hasNext()) {

			final Relationship rel = hits.next();

			hits.close();

			return rel;
		}

		if(hits != null) {

			hits.close();
		}

		return null;
	}

	public Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index, final VersionHandler versionHandler) {

		final RelationshipType relType = DynamicRelationshipType.withName(statement.getPredicate().getUri());
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (statement.getOrder() != null) {

			rel.setProperty(GraphStatics.ORDER_PROPERTY, statement.getOrder());
		}

		rel.setProperty(GraphStatics.INDEX_PROPERTY, index);

		// TODO: versioning handling only implemented for data models right now

		if (statement.getEvidence() != null) {

			rel.setProperty(GraphStatics.EVIDENCE_PROPERTY, statement.getEvidence());
		}

		if(statement.getConfidence() != null) {

			rel.setProperty(GraphStatics.CONFIDENCE_PROPERTY, statement.getConfidence());
		}

		return rel;
	}

	protected abstract IndexHits<Node> getResourceNodeHits(final ResourceNode resource);

	public abstract void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI);

	public abstract void handleObjectDataModel(Node node, String dataModelURI);

	public abstract void handleSubjectDataModel(final Node node, String URI, final String dataModelURI);

	public abstract void addStatementToIndex(final Relationship rel, final String statementUUID);

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

	private String getIdentifier(final Node node, final org.dswarm.graph.json.NodeType nodeType) {

		final String identifier;

		switch (nodeType) {

			case Resource:

				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				final String dataModel = (String) node.getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

				if (dataModel == null) {

					identifier = uri;
				} else {

					identifier = uri + dataModel;
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
