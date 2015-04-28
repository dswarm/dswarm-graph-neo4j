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
package org.dswarm.graph;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.github.emboss.siphash.SipHash;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.hp.hpl.jena.vocabulary.RDFS;
import org.mapdb.DB;
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

import org.dswarm.common.types.Tuple;
import org.dswarm.common.web.URI;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.index.MapDBUtils;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.Statement;
import org.dswarm.graph.utils.GraphDatabaseUtils;
import org.dswarm.graph.utils.NamespaceUtils;
import org.dswarm.graph.versioning.VersionHandler;

/**
 * @author tgaengler
 */
public abstract class Neo4jProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(Neo4jProcessor.class);

	protected int addedLabels = 0;

	protected final GraphDatabaseService database;
	private         Index<Node>          resources;
	private         Index<Node>          resourcesWDataModel;
	private         Index<Node>          resourceTypes;
	private         Index<Node>          values;
	private Index<Relationship> statementUUIDs;
	protected final Map<String, Node>    bnodes;

	// protected Index<Relationship> statementHashes;
	final private Set<Long> statementHashes;
	final private DB        statementHashesDB;

	final private Set<Long> tempStatementHashes;
	final private DB        tempStatementHashesDB;

	// for caching per TX
	final private Map<String, String> tempNamespacePrefixes;
	final private DB        tempNamespacePrefixesDB;

	// for caching over the whole process
	final private Map<String, String> inMemoryNamespacePrefixes;
	final private DB        inMemoryNamespacePrefixesDB;

	final private Map<String, String> namespacePrefixes;
	final private DB        namespacePrefixesDB;

	private final Map<String, String> uriPrefixedURIMap;

	protected final LongObjectMap<String> nodeResourceMap;

	// TODO: go offheap, if maps get to big
	final private Map<String, Node> tempResourcesIndex;
	final private Map<String, Node> tempResourcesWDataModelIndex;
	final private Map<String, Node> tempResourceTypesIndex;

	private final Map<String, Label> labelCache;

	protected Transaction tx;

	boolean txIsClosed = false;

	public Neo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

		this.database = database;

		tempResourcesIndex = Maps.newHashMap();
		tempResourcesWDataModelIndex = Maps.newHashMap();
		tempResourceTypesIndex = Maps.newHashMap();

		uriPrefixedURIMap = Maps.newHashMap();

		labelCache = Maps.newHashMap();

		beginTx();

		LOG.debug("start write TX");

		bnodes = new HashMap<>();
		nodeResourceMap = new LongObjectOpenHashMap<>();

		try {

			final Tuple<Set<Long>, DB> mapDBTuple = MapDBUtils
					.createOrGetInMemoryLongIndexTreeSetNonTransactional(GraphIndexStatics.TEMP_STATEMENT_HASHES_INDEX_NAME);
			tempStatementHashes = mapDBTuple.v1();
			tempStatementHashesDB = mapDBTuple.v2();

			final Tuple<Set<Long>, DB> mapDBTuple2 = getOrCreateLongIndex(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);
			statementHashes = mapDBTuple2.v1();
			statementHashesDB = mapDBTuple2.v2();

			final Tuple<Map<String, String>, DB> mapDBTuple3 = MapDBUtils.createOrGetInMemoryStringStringIndexTreeMapNonTransactional(
					GraphIndexStatics.TEMP_NAMESPACE_PREFIXES_INDEX_NAME);
			tempNamespacePrefixes = mapDBTuple3.v1();
			tempNamespacePrefixesDB = mapDBTuple3.v2();

			final Tuple<Map<String, String>, DB> mapDBTuple4 = MapDBUtils.createOrGetInMemoryStringStringIndexTreeMapNonTransactional(
					GraphIndexStatics.IN_MEMORY_NAMESPACE_PREFIXES_INDEX_NAME);
			inMemoryNamespacePrefixes = mapDBTuple4.v1();
			inMemoryNamespacePrefixesDB = mapDBTuple4.v2();

			final Tuple<Map<String, String>, DB> mapDBTuple5 = getOrCreateStringStringIndex(GraphIndexStatics.NAMESPACE_PREFIXES_INDEX_NAME);
			namespacePrefixes = mapDBTuple5.v1();
			namespacePrefixesDB = mapDBTuple5.v2();
		} catch (final IOException e) {

			failTx();

			throw new DMPGraphException("couldn't create or get statement hashes index");
		}
	}

	protected void initIndices() throws DMPGraphException {

		try {

			resources = database.index().forNodes(GraphIndexStatics.RESOURCES_INDEX_NAME);
			resourcesWDataModel = database.index().forNodes(GraphIndexStatics.RESOURCES_W_DATA_MODEL_INDEX_NAME);
			resourceTypes = database.index().forNodes(GraphIndexStatics.RESOURCE_TYPES_INDEX_NAME);
			values = database.index().forNodes(GraphIndexStatics.VALUES_INDEX_NAME);
			statementUUIDs = database.index().forRelationships(GraphIndexStatics.STATEMENT_UUIDS_INDEX_NAME);
			// statementHashes = database.index().forRelationships(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);

			tempResourcesIndex.clear();
			tempResourcesWDataModelIndex.clear();
			tempResourceTypesIndex.clear();

			if (tempStatementHashes != null) {

				tempStatementHashes.clear();
			}

			if(tempNamespacePrefixes != null) {

				tempNamespacePrefixes.clear();
			}
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

	public void addNodeToBNodesIndex(final String key, final Node bnode) {

		bnodes.put(key, bnode);
	}

	public void addNodeToValueIndex(final Node literal, final String key, final String value) {

		values.putIfAbsent(literal, key, value);
	}

	public void addHashToStatementIndex(final long hash) {

		tempStatementHashes.add(hash);
	}

	public Optional<String> optionalCreatePrefixedURI(final Optional<String> optionalFullURI) throws DMPGraphException {

		if(!optionalFullURI.isPresent()) {

			return Optional.absent();
		}

		return Optional.of(createPrefixedURI(optionalFullURI.get()));
	}

	public String createPrefixedURI(final String fullURI) throws DMPGraphException {

		if(fullURI == null) {

			throw new DMPGraphException("full URI shouldn't be null");
		}

		if(!uriPrefixedURIMap.containsKey(fullURI)) {

			final Tuple<String, String> uriParts = URI.determineParts(fullURI);
			final String namespaceURI = uriParts.v1();
			final String localName = uriParts.v2();

			final String prefix = NamespaceUtils.getPrefix(namespaceURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, namespacePrefixes);

			final String prefixedURI = prefix + NamespaceUtils.PREFIX_DELIMITER + localName;

			uriPrefixedURIMap.put(fullURI, prefixedURI);
		}

		return uriPrefixedURIMap.get(fullURI);
	}

	public String getRDFCLASSPrefixedURI() throws DMPGraphException {

		return createPrefixedURI(RDFS.Class.getURI());
	}

	public void removeHashFromStatementIndex(final long hash) {

		// TODO: maybe cache removals and remove them in one rush
		if(statementHashes.contains(hash)) {

			statementHashes.remove(hash);
		}
	}

	public void addStatementToIndex(final Relationship rel, final long statementUUID) {

		statementUUIDs.putIfAbsent(rel, GraphStatics.UUID, statementUUID);
	}

	public void clearMaps() {

		nodeResourceMap.clear();
		bnodes.clear();

		tempResourcesIndex.clear();
		tempResourcesWDataModelIndex.clear();
		tempResourceTypesIndex.clear();

		uriPrefixedURIMap.clear();

		labelCache.clear();

		LOG.debug("start clearing and closing mapdb indices");

		if (!tempStatementHashesDB.isClosed()) {

			tempStatementHashes.clear();
			tempStatementHashesDB.close();
		}

		if (!statementHashesDB.isClosed()) {

			statementHashesDB.close();
		}

		if(!tempNamespacePrefixesDB.isClosed()) {

			tempNamespacePrefixes.clear();
			tempNamespacePrefixesDB.close();
		}

		if(!inMemoryNamespacePrefixesDB.isClosed()) {

			inMemoryNamespacePrefixes.clear();
			inMemoryNamespacePrefixesDB.close();
		}

		if(!namespacePrefixesDB.isClosed()) {

			namespacePrefixesDB.commit();
			namespacePrefixesDB.close();
		}

		LOG.debug("finished clearing and closing mapdb indices");
	}

	public void beginTx() throws DMPGraphException {

		Neo4jProcessor.LOG.debug("beginning new tx");

		tx = database.beginTx();
		initIndices();
		txIsClosed = false;

		Neo4jProcessor.LOG.debug("new tx is ready");
	}

	public void renewTx() throws DMPGraphException {

		succeedTx();
		beginTx();
	}

	public void failTx() {

		Neo4jProcessor.LOG.error("tx failed; closing tx");

		tempStatementHashesDB.close();
		statementHashesDB.close();
		tempNamespacePrefixesDB.close();
		inMemoryNamespacePrefixesDB.close();
		namespacePrefixesDB.close();
		tx.failure();
		tx.close();
		txIsClosed = true;

		Neo4jProcessor.LOG.error("tx failed; closed tx");
	}

	public void succeedTx() {

		Neo4jProcessor.LOG.debug("tx succeeded; closing tx");

		pumpNFlushStatementIndex();
		pumpNFlushNamespacePrefixIndex();
		tx.success();
		tx.close();
		txIsClosed = true;

		Neo4jProcessor.LOG.debug("tx succeeded; closed tx");
	}

	public void ensureRunningTx() throws DMPGraphException {

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

		if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

			// resource node

			final Optional<Node> optionalNode;

			if (!NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

				if (!optionalDataModelURI.isPresent()) {

					optionalNode = getResourceNodeHits(optionalResourceURI.get());
				} else {

					optionalNode = getNodeFromResourcesWDataModelIndex(optionalResourceURI.get(), optionalDataModelURI.get());
				}
			} else {

				optionalNode = getNodeFromResourceTypesIndex(optionalResourceURI.get());
			}

			return optionalNode;
		}

		if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

			// literal node - should never be the case

			return Optional.absent();
		}

		// resource must be a blank node

		final Node node = bnodes.get(optionalResourceId.get());

		return Optional.fromNullable(node);
	}

	public Optional<String> determineResourceUri(final Node subjectNode, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final long nodeId = subjectNode.getId();

		final Optional<String> optionalResourceUri;

		if (nodeResourceMap.containsKey(nodeId)) {

			optionalResourceUri = Optional.of(nodeResourceMap.get(nodeId));
		} else {

			optionalResourceUri = determineResourceUri(optionalSubjectNodeType, optionalSubjectURI, optionalResourceURI);

			if (optionalResourceUri.isPresent()) {

				nodeResourceMap.put(nodeId, optionalResourceUri.get());
			}
		}

		return optionalResourceUri;
	}

	public Optional<String> determineResourceUri(final Optional<NodeType> optionalSubjectNodeType, final Optional<String> optionalSubjectURI,
			final Optional<String> optionalResourceURI) {

		final Optional<String> optionalResourceUri;

		if (optionalSubjectNodeType.isPresent()
				&& (NodeType.Resource.equals(optionalSubjectNodeType.get()) || NodeType.TypeResource.equals(optionalSubjectNodeType.get()))) {

			optionalResourceUri = optionalSubjectURI;
		} else if (optionalResourceURI.isPresent()) {

			optionalResourceUri = optionalResourceURI;
		} else {

			// shouldn't never be the case

			return Optional.absent();
		}

		return optionalResourceUri;
	}

	public void addLabel(final Node node, final String labelString) {

		final Label label = getLabel(labelString);
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

	public Label getLabel(final String labelString) {

		if(!labelCache.containsKey(labelString)) {

			labelCache.put(labelString, DynamicLabel.label(labelString));
		}

		return labelCache.get(labelString);
	}

	public boolean checkStatementExists(final long hash) throws DMPGraphException {

		return !(statementHashes == null && tempStatementHashes == null) && (tempStatementHashes != null && tempStatementHashes.contains(hash)
				|| statementHashes != null && statementHashes.contains(hash));

	}

	public Relationship prepareRelationship(final Node subjectNode, final String predicateURI, final Node objectNode, final long statementUUID,
			final Optional<Map<String, Object>> optionalQualifiedAttributes, final VersionHandler versionHandler) {

		final RelationshipType relType = DynamicRelationshipType.withName(predicateURI);
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (optionalQualifiedAttributes.isPresent()) {

			final Map<String, Object> qualifiedAttributes = optionalQualifiedAttributes.get();

			if (qualifiedAttributes.containsKey(GraphStatics.ORDER_PROPERTY)) {

				rel.setProperty(GraphStatics.ORDER_PROPERTY, qualifiedAttributes.get(GraphStatics.ORDER_PROPERTY));
			}

			if (qualifiedAttributes.containsKey(GraphStatics.INDEX_PROPERTY)) {

				rel.setProperty(GraphStatics.INDEX_PROPERTY, qualifiedAttributes.get(GraphStatics.INDEX_PROPERTY));
			}

			// TODO: versioning handling only implemented for data models right now

			if (qualifiedAttributes.containsKey(GraphStatics.EVIDENCE_PROPERTY)) {

				rel.setProperty(GraphStatics.EVIDENCE_PROPERTY, qualifiedAttributes.get(GraphStatics.EVIDENCE_PROPERTY));
			}

			if (qualifiedAttributes.containsKey(GraphStatics.CONFIDENCE_PROPERTY)) {

				rel.setProperty(GraphStatics.CONFIDENCE_PROPERTY, qualifiedAttributes.get(GraphStatics.CONFIDENCE_PROPERTY));
			}
		}

		return rel;
	}

	public long generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode, final NodeType subjectNodeType,
			final NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = Optional.fromNullable(subjectNodeType);
		final Optional<NodeType> optionalObjectNodeType = Optional.fromNullable(objectNodeType);
		final Optional<String> optionalSubjectIdentifier = getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = getIdentifier(objectNode, optionalObjectNodeType);

		return generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public long generateStatementHash(final Node subjectNode, final Statement statement, final Optional<String> optionalPrefixedPredicateURI) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = statement.getOptionalSubjectNodeType();
		final Optional<NodeType> optionalObjectNodeType = statement.getOptionalObjectNodeType();
		final Optional<String> optionalSubjectIdentifier = getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = statement.getOptionalObjectValue();
		final String predicateName = optionalPrefixedPredicateURI.get();

		return generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType,
				optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public long generateStatementHash(final String predicateName, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<NodeType> optionalObjectNodeType, final Optional<String> optionalSubjectIdentifier,
			final Optional<String> optionalObjectIdentifier) throws DMPGraphException {

		if (!optionalSubjectNodeType.isPresent() || !optionalObjectNodeType.isPresent() || !optionalSubjectIdentifier.isPresent()
				|| !optionalObjectIdentifier.isPresent()) {

			final String message = "cannot generate statement hash, because the subject node type or object node type or subject identifier or object identifier is not present";

			Neo4jProcessor.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final String simpleHashString = optionalSubjectNodeType.get().toString() + ":" + optionalSubjectIdentifier.get() + " " + predicateName + " "
				+ optionalObjectNodeType.get().toString() + ":" + optionalObjectIdentifier.get();

		final String hashString = putSaltToStatementHash(simpleHashString);

		return SipHash.digest(HashUtils.SPEC_KEY, hashString.getBytes(Charsets.UTF_8));
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

	public abstract void addObjectToResourceWDataModelIndex(final Node node, final String URI, final Optional<String> optionalDataModelURI);

	public abstract void handleObjectDataModel(Node node, Optional<String> optionalDataModelURI);

	public abstract void handleSubjectDataModel(final Node node, String URI, final Optional<String> optionalDataModelURI);

	public abstract Optional<Node> getResourceNodeHits(final String resourceURI);

	public Optional<Node> getNodeFromResourcesIndex(final String key) {

		return getNodeFromIndex(key, tempResourcesIndex, resources, GraphStatics.URI);
	}

	public Optional<Node> getNodeFromResourceTypesIndex(final String key) {

		return getNodeFromIndex(key, tempResourceTypesIndex, resourceTypes, GraphStatics.URI);
	}

	public Optional<Node> getNodeFromResourcesWDataModelIndex(final String resourceUri, final String dataModelUri) {

		return getNodeFromIndex(resourceUri + dataModelUri, tempResourcesWDataModelIndex, resourcesWDataModel, GraphStatics.URI_W_DATA_MODEL);
	}

	public Optional<Relationship> getRelationshipFromStatementIndex(final String uuid) {

		if(statementUUIDs == null) {

			return Optional.absent();
		}

		final IndexHits<Relationship> hits = statementUUIDs.get(GraphStatics.UUID, uuid);

		if (hits != null && hits.hasNext()) {

			final Relationship rel = hits.next();

			hits.close();

			return Optional.of(rel);
		}

		if (hits != null) {

			hits.close();
		}

		return Optional.absent();
	}

	public void addNodeToResourcesIndex(final String value, final Node node) {

		addNodeToIndex(GraphStatics.URI, value, node, tempResourcesIndex, resources);
	}

	public void addNodeToResourcesWDataModelIndex(final String resourceUri, final String dataModelUri, final Node node) {

		addNodeToIndex(GraphStatics.URI_W_DATA_MODEL, resourceUri + dataModelUri, node, tempResourcesWDataModelIndex, resourcesWDataModel);
		addNodeToResourcesIndex(resourceUri, node);
	}

	public void addNodeToResourceTypesIndex(final String key, final Node node) {

		addNodeToIndex(GraphStatics.URI, key, node, tempResourceTypesIndex, resourceTypes);
		addNodeToResourcesIndex(key, node);
	}

	protected abstract String putSaltToStatementHash(final String hash);

	protected Optional<Node> getNodeFromIndex(final String key, final Map<String, Node> tempIndex, final Index<Node> index,
			final String indexProperty) {

		if (tempIndex.containsKey(key)) {

			return Optional.of(tempIndex.get(key));
		}

		if (index == null) {

			return Optional.absent();
		}

		final IndexHits<Node> hits = index.get(indexProperty, key);

		if (hits != null && hits.hasNext()) {

			final Node hit = hits.next();

			hits.close();

			final Optional<Node> optionalHit = Optional.fromNullable(hit);

			if (optionalHit.isPresent()) {

				// temp cache index hits again
				tempIndex.put(key, optionalHit.get());
			}

			return optionalHit;
		}

		if (hits != null) {

			hits.close();
		}

		return Optional.absent();
	}

	protected Tuple<Set<Long>, DB> getOrCreateLongIndex(final String name) throws IOException {

		final String storeDir = GraphDatabaseUtils.determineMapDBIndexStoreDir(database);

		return MapDBUtils.createOrGetPersistentLongIndexTreeSetGlobalTransactional(storeDir + File.separator + name, name);
	}

	protected Tuple<Map<String, String>, DB> getOrCreateStringStringIndex(final String name) throws IOException {

		final String storeDir = GraphDatabaseUtils.determineMapDBIndexStoreDir(database);

		return MapDBUtils.createOrGetPersistentStringStringIndexTreeMapGlobalTransactional(storeDir + File.separator + name, name);
	}

	private void addNodeToIndex(final String indexProperty, final String key, final Node node, final Map<String, Node> tempIndex,
			final Index<Node> index) {

		tempIndex.put(key, node);

		// TODO: we probably shall do this at the end of the transaction, or?
		index.putIfAbsent(node, indexProperty, key);
	}

	private void pumpNFlushStatementIndex() {

		LOG.debug("start pump'n'flushing statement index; size = '{}'", tempStatementHashes.size());

		for (final Long hash : tempStatementHashes) {

			statementHashes.add(hash);
		}

		LOG.debug("finished pumping statement index");

		tempStatementHashesDB.commit();
		//tempStatementHashes.clear();
		//tempStatementHashesDB.close();
		statementHashesDB.commit();
		//statementHashesDB.close();

		LOG.debug("finished flushing statement index");
	}

	private void pumpNFlushNamespacePrefixIndex() {

		LOG.debug("start pump'n'flushing namespace prefix index; size = '{}'", tempNamespacePrefixes.size());

		for (final Map.Entry<String, String> entry : tempNamespacePrefixes.entrySet()) {

			final String namespace = entry.getKey();
			final String prefix = entry.getValue();

			inMemoryNamespacePrefixes.put(namespace, prefix);
			namespacePrefixes.put(namespace, prefix);
		}

		LOG.debug("finished pumping namespace prefix index");

		tempNamespacePrefixesDB.commit();
		//tempStatementHashes.clear();
		//tempStatementHashesDB.close();
		inMemoryNamespacePrefixesDB.commit();
		namespacePrefixesDB.commit();
		//statementHashesDB.close();

		LOG.debug("finished flushing namespace prefix index");
	}
}
