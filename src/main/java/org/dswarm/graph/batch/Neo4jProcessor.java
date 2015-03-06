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
/**
 * This file is part of d:swarm graph extension. d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version. d:swarm graph extension is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with d:swarm
 * graph extension. If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.batch;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import net.openhft.chronicle.map.ChronicleMap;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.index.ChronicleMapUtils;
import org.dswarm.graph.model.GraphStatics;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongLongOpenHashMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.github.emboss.siphash.SipHash;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Resources;

/**
 * @author tgaengler
 */
public abstract class Neo4jProcessor {

	private static final Logger						LOG			= LoggerFactory.getLogger(Neo4jProcessor.class);

	protected int									addedLabels	= 0;

	protected final BatchInserter					inserter;
	private BatchInserterIndex						resources;
	private BatchInserterIndex						resourcesWDataModel;
	private BatchInserterIndex						resourceTypes;

	protected final ObjectLongOpenHashMap<String>	tempResourcesIndex;
	protected final ObjectLongOpenHashMap<String>	tempResourcesWDataModelIndex;
	protected final ObjectLongOpenHashMap<String>	tempResourceTypes;

	private BatchInserterIndex						values;
	protected final ObjectLongOpenHashMap<String>	bnodes;
	// private BatchInserterIndex statementHashes;
	private ChronicleMap<Long, Long>				statementHashes;

	// protected final LongLongOpenHashMap tempStatementHashes;

	protected final LongObjectOpenHashMap<String>	nodeResourceMap;

	public Neo4jProcessor(final BatchInserter inserter) throws DMPGraphException {

		this.inserter = inserter;

		Neo4jProcessor.LOG.debug("start writing");

		bnodes = new ObjectLongOpenHashMap<>();
		nodeResourceMap = new LongObjectOpenHashMap<>();

		tempResourcesIndex = new ObjectLongOpenHashMap<>();
		tempResourcesWDataModelIndex = new ObjectLongOpenHashMap<>();
		tempResourceTypes = new ObjectLongOpenHashMap<>();
		// tempStatementHashes = new LongLongOpenHashMap();

		try {

			statementHashes = getOrCreateLongIndex(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);
		} catch (final IOException e) {

			throw new DMPGraphException("couldn't create or get statement hashes index");
		}

		// TODO: init all indices, when batch inserter should work on a pre-filled database (otherwise, the existing index would
		// utilised in the first run)
		// initIndices();
		initValueIndex();
	}

	protected void pumpNFlushNClearIndices() {

		Neo4jProcessor.LOG.debug("start pumping indices");

		copyNFlushNClearIndex(tempResourcesIndex, resources, GraphStatics.URI, GraphIndexStatics.RESOURCES_INDEX_NAME);
		copyNFlushNClearIndex(tempResourcesWDataModelIndex, resourcesWDataModel, GraphStatics.URI_W_DATA_MODEL,
				GraphIndexStatics.RESOURCES_W_DATA_MODEL_INDEX_NAME);
		copyNFlushNClearIndex(tempResourceTypes, resourceTypes, GraphStatics.URI, GraphIndexStatics.RESOURCE_TYPES_INDEX_NAME);
		// copyNFlushNClearLongIndex(tempStatementHashes, statementHashes, GraphStatics.HASH,
		// GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);

		Neo4jProcessor.LOG.debug("finished pumping indices");
	}

	private void copyNFlushNClearIndex(final ObjectLongOpenHashMap<String> tempIndex, final BatchInserterIndex neo4jIndex,
			final String indexProperty, final String indexName) {

		Neo4jProcessor.LOG.debug("start pumping '" + indexName + "' index of size '" + tempIndex.size() + "'");

		final Object[] keys = tempIndex.keys;
		final long[] values = tempIndex.values;
		final boolean[] states = tempIndex.allocated;

		Neo4jProcessor.LOG.debug("keys size = '" + keys.length + "' :: values size = '" + values.length + "' :: states size = '" + states.length
				+ "'");

		int j = 0;
		long tick = System.currentTimeMillis();
		int sinceLast = 0;

		for (int i = 0; i < states.length; i++) {

			if (states[i]) {

				// @tgaengler: I can't remember why I'm utilising an char array here ...
				neo4jIndex.add(values[i], MapUtil.map(indexProperty, keys[i].toString().toCharArray()));

				j++;

				final int entryDelta = j - sinceLast;
				final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

				if (entryDelta >= 1000000 || timeDelta >= 60) {

					sinceLast = j;

					Neo4jProcessor.LOG.debug("wrote '" + j + "' entries @ ~" + (double) entryDelta / timeDelta + " entries/second.");

					tick = System.currentTimeMillis();
				}
			}
		}

		Neo4jProcessor.LOG.debug("finished pumping '" + indexName + "' index; wrote '" + j + "' entries");

		Neo4jProcessor.LOG.debug("start flushing and clearing index");

		neo4jIndex.flush();
		tempIndex.clear();

		Neo4jProcessor.LOG.debug("finished flushing and clearing index");
	}

	private void copyNFlushNClearLongIndex(final LongLongOpenHashMap tempIndex, final BatchInserterIndex neo4jIndex, final String indexProperty,
			final String indexName) {

		Neo4jProcessor.LOG.debug("start pumping '" + indexName + "' index of size '" + tempIndex.size() + "'");

		final long[] keys = tempIndex.keys;
		final long[] values = tempIndex.values;
		final boolean[] states = tempIndex.allocated;

		Neo4jProcessor.LOG.debug("keys size = '" + keys.length + "' :: values size = '" + values.length + "' :: states size = '" + states.length
				+ "'");

		int j = 0;
		long tick = System.currentTimeMillis();
		int sinceLast = 0;

		for (int i = 0; i < states.length; i++) {

			if (states[i]) {

				neo4jIndex.add(values[i], MapUtil.map(indexProperty, keys[i]));

				j++;

				final int entryDelta = j - sinceLast;
				final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

				if (entryDelta >= 1000000 || timeDelta >= 60) {

					sinceLast = j;

					Neo4jProcessor.LOG.debug("wrote '" + j + "' entries @ ~" + (double) entryDelta / timeDelta + " entries/second.");

					tick = System.currentTimeMillis();
				}
			}
		}

		Neo4jProcessor.LOG.debug("finished pumping index '" + indexName + "' index; wrote '" + j + "' entries");

		Neo4jProcessor.LOG.debug("start flushing and clearing index");

		neo4jIndex.flush();
		tempIndex.clear();

		Neo4jProcessor.LOG.debug("finished flushing and clearing index");
	}

	protected void initValueIndex() throws DMPGraphException {

		try {

			values = getOrCreateIndex(GraphIndexStatics.VALUES_INDEX_NAME, GraphStatics.VALUE, true, 1);
		} catch (final Exception e) {

			final String message = "couldn't load indices successfully";

			Neo4jProcessor.LOG.error(message, e);
			Neo4jProcessor.LOG.debug("couldn't finish writing successfully");

			throw new DMPGraphException(message);
		}
	}

	protected void initIndices() throws DMPGraphException {

		try {

			resources = getOrCreateIndex(GraphIndexStatics.RESOURCES_INDEX_NAME, GraphStatics.URI, true, 1);
			resourcesWDataModel = getOrCreateIndex(GraphIndexStatics.RESOURCES_W_DATA_MODEL_INDEX_NAME, GraphStatics.URI_W_DATA_MODEL, true, 1);
			resourceTypes = getOrCreateIndex(GraphIndexStatics.RESOURCE_TYPES_INDEX_NAME, GraphStatics.URI, true, 1);
			// statementHashes = getOrCreateIndex(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME, GraphStatics.HASH, false,
			// 1000000);
		} catch (final Exception e) {

			final String message = "couldn't load indices successfully";

			Neo4jProcessor.LOG.error(message, e);
			Neo4jProcessor.LOG.debug("couldn't finish writing successfully");

			throw new DMPGraphException(message);
		}
	}

	public BatchInserter getBatchInserter() {

		return inserter;
	}

	public void addToResourcesIndex(final String key, final long nodeId) {

		tempResourcesIndex.put(key, nodeId);
	}

	public Optional<Long> getNodeIdFromResourcesIndex(final String key) {

		return getIdFromIndex(key, tempResourcesIndex, resources, GraphStatics.URI);
	}

	public void addToResourcesWDataModelIndex(final String key, final long nodeId) {

		tempResourcesWDataModelIndex.put(key, nodeId);
	}

	public Optional<Long> getNodeIdFromResourcesWDataModelIndex(final String key) {

		return getIdFromIndex(key, tempResourcesWDataModelIndex, resourcesWDataModel, GraphStatics.URI_W_DATA_MODEL);
	}

	public void addToBNodesIndex(final String key, final long nodeId) {

		bnodes.put(key, nodeId);
	}

	public Optional<Long> getNodeIdFromBNodesIndex(final String key) {

		if (key == null) {

			return Optional.absent();
		}

		if (bnodes.containsKey(key)) {

			return Optional.of(bnodes.lget());
		}

		return Optional.absent();
	}

	public void addToResourceTypesIndex(final String key, final long nodeId) {

		tempResourceTypes.put(key, nodeId);
	}

	public Optional<Long> getNodeIdFromResourceTypesIndex(final String key) {

		return getIdFromIndex(key, tempResourceTypes, resourceTypes, GraphStatics.URI);
	}

	public void addToValueIndex(final String key, final long nodeId) {

		values.add(nodeId, MapUtil.map(GraphStatics.VALUE, key));
	}

	public void addToStatementIndex(final long key, final long nodeId) {

		// tempStatementHashes.put(key, nodeId);
		statementHashes.acquireUsing(key, nodeId);
	}

	public void flushIndices() throws DMPGraphException {

		Neo4jProcessor.LOG.debug("start flushing indices");

		if (resources == null) {

			initIndices();
		}

		pumpNFlushNClearIndices();
		flushStatementIndices();
		clearTempIndices();

		Neo4jProcessor.LOG.debug("start finished flushing indices");
	}

	public void flushStatementIndices() {

		// statementHashes.flush();
	}

	protected void clearTempIndices() {

		clearTempStatementIndices();
	}

	protected void clearTempStatementIndices() {

		// tempStatementHashes.clear();
	}

	public void clearMaps() {

		nodeResourceMap.clear();
		bnodes.clear();
	}

	public Optional<Long> determineNode(final Optional<NodeType> optionalResourceNodeType, final Optional<String> optionalResourceId,
			final Optional<String> optionalResourceURI, final Optional<String> optionalDataModelURI) {

		if (!optionalResourceNodeType.isPresent()) {

			return Optional.absent();
		}

		if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

			// resource node

			final Optional<Long> optionalNodeId;

			if (!NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

				if (!optionalDataModelURI.isPresent()) {

					optionalNodeId = getResourceNodeHits(optionalResourceURI.get());
				} else {

					optionalNodeId = getNodeIdFromResourcesWDataModelIndex(optionalResourceURI.get() + optionalDataModelURI.get());
				}
			} else {

				optionalNodeId = getNodeIdFromResourceTypesIndex(optionalResourceURI.get());
			}

			return optionalNodeId;
		}

		if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

			// literal node - should never be the case

			return Optional.absent();
		}

		// resource must be a blank node

		return getNodeIdFromBNodesIndex(optionalResourceId.get());
	}

	public Optional<String> determineResourceUri(final long subjectNodeId, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final Optional<String> optionalResourceUri;

		if (nodeResourceMap.containsKey(subjectNodeId)) {

			optionalResourceUri = Optional.of(nodeResourceMap.lget());
		} else {

			optionalResourceUri = determineResourceUri(optionalSubjectNodeType, optionalSubjectURI, optionalResourceURI);

			if (optionalResourceUri.isPresent()) {

				nodeResourceMap.put(subjectNodeId, optionalResourceUri.get());
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

	public void addLabel(final long nodeId, final String labelString) {

		final Label label = DynamicLabel.label(labelString);

		inserter.setNodeLabels(nodeId, label);
	}

	public Optional<Long> getStatement(final long hash) throws DMPGraphException {

		return getIdFromLongIndex(hash, statementHashes);
	}

	public Map<String, Object> prepareRelationship(final String statementUUID, final Optional<Map<String, Object>> optionalQualifiedAttributes) {

		final Map<String, Object> relProperties = new HashMap<>();

		relProperties.put(GraphStatics.UUID_PROPERTY, statementUUID);

		if (optionalQualifiedAttributes.isPresent()) {

			final Map<String, Object> qualifiedAttributes = optionalQualifiedAttributes.get();

			if (qualifiedAttributes.containsKey(GraphStatics.ORDER_PROPERTY)) {

				relProperties.put(GraphStatics.ORDER_PROPERTY, qualifiedAttributes.get(GraphStatics.ORDER_PROPERTY));
			}

			if (qualifiedAttributes.containsKey(GraphStatics.INDEX_PROPERTY)) {

				relProperties.put(GraphStatics.INDEX_PROPERTY, qualifiedAttributes.get(GraphStatics.INDEX_PROPERTY));
			}

			// TODO: versioning handling only implemented for data models right now

			if (qualifiedAttributes.containsKey(GraphStatics.EVIDENCE_PROPERTY)) {

				relProperties.put(GraphStatics.EVIDENCE_PROPERTY, qualifiedAttributes.get(GraphStatics.EVIDENCE_PROPERTY));
			}

			if (qualifiedAttributes.containsKey(GraphStatics.CONFIDENCE_PROPERTY)) {

				relProperties.put(GraphStatics.CONFIDENCE_PROPERTY, qualifiedAttributes.get(GraphStatics.CONFIDENCE_PROPERTY));
			}
		}

		return relProperties;
	}

	public long generateStatementHash(final long subjectNodeId, final String predicateName, final long objectNodeId, final NodeType subjectNodeType,
			final NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = Optional.fromNullable(subjectNodeType);
		final Optional<NodeType> optionalObjectNodeType = Optional.fromNullable(objectNodeType);
		final Optional<String> optionalSubjectIdentifier = getIdentifier(subjectNodeId, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = getIdentifier(objectNodeId, optionalObjectNodeType);

		return generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public long generateStatementHash(final long subjectNodeId, final String predicateName, final String objectValue, final NodeType subjectNodeType,
			final NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = Optional.fromNullable(subjectNodeType);
		final Optional<NodeType> optionalObjectNodeType = Optional.fromNullable(objectNodeType);
		final Optional<String> optionalSubjectIdentifier = getIdentifier(subjectNodeId, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = Optional.fromNullable(objectValue);

		return generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
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

		final String hashString = optionalSubjectNodeType.toString() + ":" + optionalSubjectIdentifier.get() + " " + predicateName + " "
				+ optionalObjectNodeType.toString() + ":" + optionalObjectIdentifier.get();

		return SipHash.digest(HashUtils.SPEC_KEY, hashString.getBytes(Charsets.UTF_8));
	}

	public Optional<String> getIdentifier(final long nodeId, final Optional<NodeType> optionalNodeType) {

		if (!optionalNodeType.isPresent()) {

			return Optional.absent();
		}

		final String identifier;

		switch (optionalNodeType.get()) {

			case Resource:
			case TypeResource:

				final String uri = (String) getProperty(GraphStatics.URI_PROPERTY, inserter.getNodeProperties(nodeId));
				final String dataModel = (String) getProperty(GraphStatics.DATA_MODEL_PROPERTY, inserter.getNodeProperties(nodeId));

				if (dataModel == null) {

					identifier = uri;
				} else {

					identifier = uri + dataModel;
				}

				break;
			case BNode:
			case TypeBNode:

				identifier = "" + nodeId;

				break;
			case Literal:

				identifier = (String) getProperty(GraphStatics.VALUE_PROPERTY, inserter.getNodeProperties(nodeId));

				break;
			default:

				identifier = null;

				break;
		}

		return Optional.fromNullable(identifier);
	}

	public abstract void addObjectToResourceWDataModelIndex(final long nodeId, final String URI, final Optional<String> optionalDataModelURI);

	public abstract void handleObjectDataModel(final Map<String, Object> objectNodeProperties, final Optional<String> optionalDataModelURI);

	public abstract void handleSubjectDataModel(final Map<String, Object> subjectNodeProperties, String URI,
			final Optional<String> optionalDataModelURI);

	public abstract void addStatementToIndex(final long relId, final String statementUUID);

	public abstract Optional<Long> getResourceNodeHits(final String resourceURI);

	protected BatchInserterIndex getOrCreateIndex(final String name, final String property, final boolean nodeIndex, final int cachSize) {

		final BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);
		final BatchInserterIndex index;

		if (nodeIndex) {

			index = indexProvider.nodeIndex(name, MapUtil.stringMap("type", "exact"));
		} else {

			index = indexProvider.relationshipIndex(name, MapUtil.stringMap("type", "exact"));
		}

		index.setCacheCapacity(property, cachSize);

		return index;
	}

	protected ChronicleMap<Long, Long> getOrCreateLongIndex(final String name) throws IOException {

		final URL resource = Resources.getResource("dmpgraph.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dmpgraph.properties", e);
		}

		final String tempUserDir = properties.getProperty("index_store_dir");

		final String storeDir;

		if (tempUserDir != null && !tempUserDir.trim().isEmpty()) {

			storeDir = tempUserDir;
		} else {

			// fallback default
			storeDir = System.getProperty("user.dir") + File.separator + inserter.getStoreDir();
		}

		// + File.separator + ChronicleMapUtils.INDEX_DIR
		return ChronicleMapUtils.createOrGetLongIndex(storeDir + File.separator + name);
	}

	private Object getProperty(final String key, final Map<String, Object> properties) {

		if (properties == null || properties.isEmpty()) {

			return null;
		}

		if (!properties.containsKey(key)) {

			return null;
		}

		return properties.get(key);
	}

	private Optional<Long> getIdFromIndex(final String key, final ObjectLongOpenHashMap<String> tempIndex, final BatchInserterIndex index,
			final String indexProperty) {

		if (key == null) {

			return Optional.absent();
		}

		if (tempIndex.containsKey(key)) {

			return Optional.of(tempIndex.lget());
		}

		if (index == null) {

			return Optional.absent();
		}

		final IndexHits<Long> hits = index.get(indexProperty, key);

		if (hits != null && hits.hasNext()) {

			final Long hit = hits.next();

			hits.close();

			final Optional<Long> optionalHit = Optional.fromNullable(hit);

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

	private Optional<Long> getIdFromLongIndex(final long key, final ChronicleMap<Long, Long> index) {

		if (index == null) {

			return Optional.absent();
		}

		return Optional.fromNullable(index.get(key));
	}
}
