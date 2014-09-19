package org.dswarm.graph;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.gdm.utils.NodeTypeUtils;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
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

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public abstract class Neo4jProcessor {

	private static final Logger				LOG			= LoggerFactory.getLogger(Neo4jProcessor.class);

	protected int							addedLabels	= 0;

	protected final GraphDatabaseService	database;
	protected final Index<Node>				resources;
	protected final Index<Node>				resourcesWDataModel;
	protected final Index<Node>				resourceTypes;
	protected final Index<Node>				values;
	protected final Map<String, Node>		bnodes;
	protected final Index<Relationship>		statementHashes;
	protected final Map<Long, String>		nodeResourceMap;

	protected Transaction					tx;

	boolean									txIsClosed	= false;

	public Neo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

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

			failTx();

			final String message = "couldn't load indices successfully";

			Neo4jProcessor.LOG.error(message, e);
			Neo4jProcessor.LOG.debug("couldn't finish write TX successfully");

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

	public Map<Long, String> getNodeResourceMap() {

		return nodeResourceMap;
	}

	public void beginTx() {

		tx = database.beginTx();
		txIsClosed = false;

		Neo4jProcessor.LOG.debug("begin new tx");
	}

	public void renewTx() {

		succeedTx();
		beginTx();
	}

	public void failTx() {

		Neo4jProcessor.LOG.error("tx failed; close tx");

		tx.failure();
		tx.close();
		txIsClosed = true;
	}

	public void succeedTx() {

		Neo4jProcessor.LOG.debug("tx succeeded; close tx");

		tx.success();
		tx.close();
		txIsClosed = true;
	}

	public void ensureRunningTx() {

		if (txIsClosed()) {

			beginTx();
		}
	}

	public boolean txIsClosed() {

		return txIsClosed;
	}

	public Optional<Node> determineNode(final Optional<NodeType> optionalResourceNodeType, final Optional<String> optionalResourceId,
			final Optional<String> optionalResourceURI, final Optional<String> optionalDataModelURI) {

		if (!optionalResourceNodeType.isPresent()) {

			return Optional.absent();
		}

		final Node node;

		if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

			// resource node

			final IndexHits<Node> hits;

			if (!NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

				if (!optionalDataModelURI.isPresent()) {

					hits = getResourceNodeHits(optionalResourceURI.get());
				} else {

					hits = resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL, optionalResourceURI.get() + optionalDataModelURI.get());
				}
			} else {

				hits = resourceTypes.get(GraphStatics.URI, optionalResourceURI.get());
			}

			if (hits != null && hits.hasNext()) {

				// node exists

				node = hits.next();

				hits.close();

				return Optional.fromNullable(node);
			}

			if (hits != null) {

				hits.close();
			}

			return Optional.absent();
		}

		if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

			// literal node - should never be the case

			return Optional.absent();
		}

		// resource must be a blank node

		node = bnodes.get(optionalResourceId.get());

		return Optional.fromNullable(node);
	}

	public Optional<String> determineResourceUri(final Node subjectNode, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final Long nodeId = subjectNode.getId();

		final String resourceUri;

		if (nodeResourceMap.containsKey(nodeId)) {

			resourceUri = nodeResourceMap.get(nodeId);
		} else {

			if (optionalSubjectNodeType.isPresent()
					&& (NodeType.Resource.equals(optionalSubjectNodeType.get()) || NodeType.TypeResource.equals(optionalSubjectNodeType.get()))) {

				resourceUri = optionalSubjectURI.get();
			} else if (optionalResourceURI.isPresent()) {

				resourceUri = optionalResourceURI.get();
			} else {

				// shouldn't never be the case

				resourceUri = null;
			}

			nodeResourceMap.put(nodeId, resourceUri);
		}

		return Optional.fromNullable(resourceUri);
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

	public Relationship getStatement(final String hash) throws DMPGraphException {

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

	public Relationship prepareRelationship(final Node subjectNode, final String predicateURI, final Node objectNode, final String statementUUID,
			final Map<String, Object> qualifiedAttributes, final long index, final VersionHandler versionHandler) {

		final RelationshipType relType = DynamicRelationshipType.withName(predicateURI);
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (qualifiedAttributes.containsKey(GraphStatics.ORDER_PROPERTY)) {

			rel.setProperty(GraphStatics.ORDER_PROPERTY, qualifiedAttributes.get(GraphStatics.ORDER_PROPERTY));
		}

		rel.setProperty(GraphStatics.INDEX_PROPERTY, index);

		// TODO: versioning handling only implemented for data models right now

		if (qualifiedAttributes.containsKey(GraphStatics.EVIDENCE_PROPERTY)) {

			rel.setProperty(GraphStatics.EVIDENCE_PROPERTY, qualifiedAttributes.get(GraphStatics.EVIDENCE_PROPERTY));
		}

		if (qualifiedAttributes.containsKey(GraphStatics.CONFIDENCE_PROPERTY)) {

			rel.setProperty(GraphStatics.CONFIDENCE_PROPERTY, qualifiedAttributes.get(GraphStatics.CONFIDENCE_PROPERTY));
		}

		return rel;
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode, final NodeType subjectNodeType,
			final NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = Optional.fromNullable(subjectNodeType);
		final Optional<NodeType> optionalObjectNodeType = Optional.fromNullable(objectNodeType);
		final Optional<String> optionalSubjectIdentifier = getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = getIdentifier(objectNode, optionalObjectNodeType);

		return generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final String objectValue, final NodeType subjectNodeType,
			final NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = Optional.fromNullable(subjectNodeType);
		final Optional<NodeType> optionalObjectNodeType = Optional.fromNullable(objectNodeType);
		final Optional<String> optionalSubjectIdentifier = getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = Optional.fromNullable(objectValue);

		return generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public String generateStatementHash(final String predicateName, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<NodeType> optionalObjectNodeType, final Optional<String> optionalSubjectIdentifier,
			final Optional<String> optionalObjectIdentifier) throws DMPGraphException {

		if (!optionalSubjectNodeType.isPresent() || !optionalObjectNodeType.isPresent() || !optionalSubjectIdentifier.isPresent()
				|| !optionalObjectIdentifier.isPresent()) {

			final String message = "cannot generate statement hash, because the subject node type or object node type or subject identifier or object identifier is not present";

			Neo4jProcessor.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final StringBuilder sb = new StringBuilder();

		sb.append(optionalSubjectNodeType.toString()).append(":").append(optionalSubjectIdentifier.get()).append(" ").append(predicateName)
				.append(" ").append(optionalObjectNodeType.toString()).append(":").append(optionalObjectIdentifier.get()).append(" ");

		MessageDigest messageDigest = null;

		try {

			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e) {

			throw new DMPGraphException("couldn't instantiate hash algo");
		}
		messageDigest.update(sb.toString().getBytes());

		return new String(messageDigest.digest());
	}

	public Optional<String> getIdentifier(final Node node, final Optional<NodeType> optionalNodeType) {

		if (!optionalNodeType.isPresent()) {

			return Optional.absent();
		}

		final String identifier;

		switch (optionalNodeType.get()) {

			case Resource:
			case TypeResource:

				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				final String dataModel = (String) node.getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

				if (dataModel == null) {

					identifier = uri;
				} else {

					identifier = uri + dataModel;
				}

				break;
			case BNode:
			case TypeBNode:

				identifier = "" + node.getId();

				break;
			case Literal:

				identifier = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);

				break;
			default:

				identifier = null;

				break;
		}

		return Optional.fromNullable(identifier);
	}

	public abstract void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI);

	public abstract void handleObjectDataModel(Node node, String dataModelURI);

	public abstract void handleSubjectDataModel(final Node node, String URI, final String dataModelURI);

	public abstract void addStatementToIndex(final Relationship rel, final String statementUUID);

	public abstract IndexHits<Node> getResourceNodeHits(final String resourceURI);
}
