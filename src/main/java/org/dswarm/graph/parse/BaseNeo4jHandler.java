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
package org.dswarm.graph.parse;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.github.emboss.siphash.SipHash;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.hp.hpl.jena.vocabulary.RDF;
import org.apache.commons.lang.NotImplementedException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.Statement;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
public abstract class BaseNeo4jHandler implements Neo4jHandler, Neo4jUpdateHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BaseNeo4jHandler.class);

	private static final int TX_CHUNK_SIZE = 50000;
	private static final int TX_TIME_DELTA = 30;

	protected int totalTriples       = 0;
	protected int addedNodes         = 0;
	protected int addedRelationships = 0;
	protected int sinceLastCommit    = 0;
	protected int i                  = 0;
	protected int literals           = 0;

	protected long tick = System.currentTimeMillis();

	protected String resourceUri;
	protected long resourceHash;

	protected AtomicLong resourceIndexCounter = new AtomicLong(0);

	// TODO: init
	protected VersionHandler versionHandler = null;

	protected final BasicNeo4jProcessor processor;

	public BaseNeo4jHandler(final BasicNeo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;

		init();
	}

	@Override
	public Neo4jProcessor getProcessor() {

		return processor;
	}

	@Override
	public void setResourceUri(final String resourceUriArg) throws DMPGraphException {

		resourceUri = resourceUriArg;
	}

	@Override
	public void setResourceHash(final long resourceHashArg) {

		resourceHash = resourceHashArg;
	}

	@Override public void resetResourceIndexCounter() {

		resourceIndexCounter = new AtomicLong(0);
	}

	@Override
	public VersionHandler getVersionHandler() {

		return versionHandler;
	}

	@Override
	public void handleStatement(Statement statement) throws DMPGraphException {

		// utilise r for the resource property

		i++;

		processor.ensureRunningTx();

		try {

			if (!statement.getOptionalSubjectNodeType().isPresent() || !statement.getOptionalPredicateURI().isPresent()
					|| !statement.getOptionalObjectNodeType().isPresent()) {

				throw new DMPGraphException("cannot handle statement, because no subject node type or predicate uri or object node type is present");
			}

			final NodeType subjectNodeType = statement.getOptionalSubjectNodeType().get();
			final NodeType objectNodeType = statement.getOptionalObjectNodeType().get();

			// Check index for subject
			// TODO: what should we do, if the subject is a resource type?
			final Optional<String> optionalPrefixedSubjectURI = processor.optionalCreatePrefixedURI(statement.getOptionalSubjectURI());
			final Optional<String> optionalPrefixedSubjectDataModelURI = processor
					.optionalCreatePrefixedURI(statement.getOptionalSubjectDataModelURI());

			final Optional<Long> optionalSubjectUriDataModelUriHash;

			if (optionalPrefixedSubjectURI.isPresent()) {

				optionalSubjectUriDataModelUriHash = Optional
						.of(processor.generateResourceHash(optionalPrefixedSubjectURI.get(), optionalPrefixedSubjectDataModelURI));
			} else {

				optionalSubjectUriDataModelUriHash = Optional.absent();
			}

			final Optional<Node> optionalSubjectNode = processor.determineNode(statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectId(), optionalPrefixedSubjectURI, optionalPrefixedSubjectDataModelURI,
					optionalSubjectUriDataModelUriHash);
			final Node subjectNode;

			if (optionalSubjectNode.isPresent()) {

				subjectNode = optionalSubjectNode.get();
			} else {

				final Label subjectLabel = processor.getLabel(subjectNodeType.toString());
				subjectNode = processor.getDatabase().createNode(subjectLabel);

				if (NodeType.Resource.equals(subjectNodeType) || NodeType.TypeResource.equals(subjectNodeType)) {

					if (NodeType.TypeResource.equals(subjectNodeType)) {

						processor.addLabel(subjectNode, NodeType.Resource.toString());
					}

					// subject is a resource node

					final String subjectURI = optionalPrefixedSubjectURI.get();

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subjectURI);
					subjectNode.setProperty(GraphStatics.HASH, optionalSubjectUriDataModelUriHash.get());

					if (resourceHash == optionalSubjectUriDataModelUriHash.get()) {

						versionHandler.setLatestVersion(optionalPrefixedSubjectDataModelURI);
					}

					processor.handleSubjectDataModel(subjectNode, subjectURI, optionalPrefixedSubjectDataModelURI);

					processor.addNodeToResourcesIndex(subjectURI, subjectNode);
				} else {

					// subject is a blank node

					// note: can I expect an id here?
					processor.addNodeToBNodesIndex(statement.getOptionalSubjectId().get(), subjectNode);
				}

				addedNodes++;
			}

			final Optional<Long> optionalStatementResourceHash;

			if(!statement.getOptionalResourceHash().isPresent()) {

				optionalStatementResourceHash = Optional.of(resourceHash);
			} else  {

				optionalStatementResourceHash = statement.getOptionalResourceHash();
			}

			final Optional<String> optionalPrefixedPredicateURI = processor.optionalCreatePrefixedURI(statement.getOptionalPredicateURI());

			if (NodeType.Literal.equals(objectNodeType)) {

				handleLiteral(subjectNode, statement, optionalSubjectUriDataModelUriHash, optionalStatementResourceHash, optionalPrefixedPredicateURI);
			} else { // must be Resource
				// Make sure object exists

				final Optional<String> optionalPrefixedObjectURI = processor.optionalCreatePrefixedURI(statement.getOptionalObjectURI());

				boolean isType = false;

				// add Label to subject node, if object is a type entry
				if (statement.getOptionalPredicateURI().get().equals(RDF.type.getURI())) {

					processor.addLabel(subjectNode, optionalPrefixedObjectURI.get());

					isType = true;
				}

				final NodeType finalObjectNodeType;

				if (!isType) {

					finalObjectNodeType = objectNodeType;
				} else {

					// correct/enhance node type

					switch (objectNodeType) {

						case Resource:

							finalObjectNodeType = NodeType.TypeResource;

							break;
						case BNode:

							finalObjectNodeType = NodeType.TypeBNode;

							break;
						default:

							finalObjectNodeType = objectNodeType;
					}
				}

				final Optional<String> optionalPrefixedObjectDataModelURI = processor
						.optionalCreatePrefixedURI(statement.getOptionalObjectDataModelURI());
				final Optional<NodeType> finalOptionalObjectNodeType = Optional.of(finalObjectNodeType);

				final Optional<Long> optionalObjectResourceUriDataModelUriHash;

				if (optionalPrefixedObjectURI.isPresent()) {

					// because type resources doesn't belong to any data model
					optionalObjectResourceUriDataModelUriHash = Optional.of(HashUtils.generateHash(optionalPrefixedObjectURI.get()));
				} else {

					optionalObjectResourceUriDataModelUriHash = Optional.absent();
				}

				// Check index for object
				final Optional<Node> optionalObjectNode = processor.determineNode(finalOptionalObjectNodeType, statement.getOptionalObjectId(),
						optionalPrefixedObjectURI, optionalPrefixedObjectDataModelURI, optionalObjectResourceUriDataModelUriHash);
				final Node objectNode;
				final Optional<Long> optionalResourceHash;

				if (optionalObjectNode.isPresent()) {

					objectNode = optionalObjectNode.get();
					optionalResourceHash = Optional.absent();
				} else {

					final Label objectLabel = processor.getLabel(finalObjectNodeType.toString());

					objectNode = processor.getDatabase().createNode(objectLabel);

					if (NodeType.Resource.equals(finalObjectNodeType) || NodeType.TypeResource.equals(finalObjectNodeType)) {

						// object is a resource node

						final String objectURI = optionalPrefixedObjectURI.get();

						objectNode.setProperty(GraphStatics.URI_PROPERTY, objectURI);
						objectNode.setProperty(GraphStatics.HASH, optionalObjectResourceUriDataModelUriHash.get());

						switch (finalObjectNodeType) {

							case Resource:

								processor.handleObjectDataModel(objectNode, optionalPrefixedObjectDataModelURI);

								break;
							case TypeResource:

								processor.addLabel(objectNode, processor.getNamespaceIndex().getRDFCLASSPrefixedURI());
								processor.addLabel(objectNode, NodeType.Resource.toString());

								processor.addNodeToResourceTypesIndex(objectURI, objectNode);

								break;
						}

						processor.addObjectToResourceWDataModelIndex(objectNode, objectURI, optionalPrefixedObjectDataModelURI);
						optionalResourceHash = Optional.absent();
					} else {

						optionalResourceHash = handleBNode(subjectNode, statement, objectNode, finalOptionalObjectNodeType,
								optionalSubjectUriDataModelUriHash,
								optionalStatementResourceHash);
					}

					addedNodes++;
				}

				// leave out, rdf:type statements for now (enable, them if footprint is not too high)
				if (!isType) {

					final long hash = processor.generateStatementHash(subjectNode, optionalPrefixedPredicateURI.get(), objectNode,
							subjectNodeType, finalObjectNodeType);

					final boolean statementExists = processor.checkStatementExists(hash);

					if (!statementExists) {

						final Optional<Long> finalOptionalResourceHash;

						if (!optionalResourceHash.isPresent()) {

							finalOptionalResourceHash = optionalStatementResourceHash;
						} else {

							finalOptionalResourceHash = optionalResourceHash;
						}

						addRelationship(subjectNode, optionalPrefixedPredicateURI.get(), objectNode, statement.getOptionalSubjectNodeType(),
								optionalSubjectUriDataModelUriHash, statement.getOptionalStatementUUID(), finalOptionalResourceHash,
								statement.getOptionalQualifiedAttributes(), hash);
					}
				}
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= TX_CHUNK_SIZE || timeDelta >= TX_TIME_DELTA) { // Commit every 50k operations or every 30 seconds

				processor.renewTx();

				sinceLastCommit = totalTriples;

				final double duration = (double) nodeDelta / timeDelta;

				LOG.debug("{} triples @ ~{} triples/second.", totalTriples, duration);

				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			final String message = "couldn't finish write TX successfully";

			LOG.error(message, e);

			processor.failTx();

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void deprecateStatement(long index) {

		throw new NotImplementedException();
	}

	@Override
	public Relationship deprecateStatement(final String uuid) throws DMPGraphException {

		processor.ensureRunningTx();

		try {

			final Optional<Relationship> optionalRel = processor.getRelationshipFromStatementIndex(uuid);

			if (!optionalRel.isPresent()) {

				BaseNeo4jHandler.LOG.error("couldn't find statement with the uuid '{}' in the database", uuid);
			}

			final Relationship rel = optionalRel.get();

			rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getLatestVersion());

			// TODO: remove statement hash from statement hashes index

			return rel;
		} catch (final Exception e) {

			final String message = "couldn't deprecate statement successfully";

			processor.failTx();

			BaseNeo4jHandler.LOG.error(message, e);
			BaseNeo4jHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void closeTransaction() throws DMPGraphException {

		LOG.debug("close write TX finally");

		processor.succeedTx();
		processor.clearMaps();
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

	protected abstract void init() throws DMPGraphException;

	public Optional<Long> handleBNode(final Node subjectNode, final Statement statement, final Node objectNode,
			final Optional<NodeType> optionalObjectNodeType, final Optional<Long> optionalSubjectHash,
			final Optional<Long> optionalResourceHash) throws DMPGraphException {

		if (!optionalObjectNodeType.isPresent()) {

			throw new DMPGraphException("there is no object node type present");
		}

		final Optional<Long> finalOptionalResourceHash;
		// object is a blank node

		processor.addNodeToBNodesIndex(statement.getOptionalObjectId().get(), objectNode);

		final NodeType objectNodeType = optionalObjectNodeType.get();

		if (!NodeType.TypeBNode.equals(objectNodeType)) {

			finalOptionalResourceHash = addResourceProperty(subjectNode, objectNode, statement.getOptionalSubjectNodeType(),
					optionalSubjectHash, optionalResourceHash);
		} else {

			processor.addLabel(objectNode, processor.getNamespaceIndex().getRDFCLASSPrefixedURI());
			processor.addLabel(objectNode, NodeType.BNode.toString());
			finalOptionalResourceHash = Optional.absent();
		}

		return finalOptionalResourceHash;
	}

	public void handleLiteral(final Node subjectNode, final Statement statement, final Optional<Long> optionalSubjectHash,
			final Optional<Long> optionalResourceHash, final Optional<String> optionalPrefixedPredicateURI) throws DMPGraphException {

		final long hash = processor.generateStatementHash(subjectNode, statement, optionalPrefixedPredicateURI);

		final boolean statementExists = processor.checkStatementExists(hash);

		if (!statementExists) {

			literals++;

			final Label objectLabel = processor.getLabel(NodeType.Literal.toString());

			final Node objectNode = processor.getDatabase().createNode(objectLabel);
			objectNode.setProperty(GraphStatics.VALUE_PROPERTY, statement.getOptionalObjectValue().get());
			//objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
			//processor.addNodeToValueIndex(objectNode, GraphStatics.VALUE, statement.getOptionalObjectValue().get());

			final Optional<Long> finalOptionalResourceHash = addResourceProperty(subjectNode, objectNode, statement.getOptionalSubjectNodeType(),
					optionalSubjectHash, optionalResourceHash);

			addedNodes++;

			addRelationship(subjectNode, optionalPrefixedPredicateURI.get(), objectNode, statement.getOptionalSubjectNodeType(),
					optionalSubjectHash, statement.getOptionalStatementUUID(), finalOptionalResourceHash,
					statement.getOptionalQualifiedAttributes(), hash);
		}
	}

	/**
	 * TODO: refactor this to protected
	 *
	 * @param subjectNode
	 * @param predicateURI
	 * @param objectNode
	 * @param optionalSubjectNodeType
	 * @param optionalSubjectHash
	 * @param optionalStatementUUID
	 * @param optionalResourceHash
	 * @param optionalQualifiedAttributes
	 * @param hash
	 * @return
	 * @throws DMPGraphException
	 */
	public Relationship addRelationship(final Node subjectNode, final String predicateURI, final Node objectNode,
			final Optional<NodeType> optionalSubjectNodeType, final Optional<Long> optionalSubjectHash,
			final Optional<String> optionalStatementUUID, final Optional<Long> optionalResourceHash,
			final Optional<Map<String, Object>> optionalQualifiedAttributes, final long hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (optionalStatementUUID.isPresent()) {

			finalStatementUUID = optionalStatementUUID.get();
		} else {

			finalStatementUUID = UUID.randomUUID().toString();
		}

		final long statementUUIDHash = SipHash.digest(HashUtils.SPEC_KEY, finalStatementUUID.getBytes(Charsets.UTF_8));

		final Relationship rel = processor.prepareRelationship(subjectNode, predicateURI, objectNode, statementUUIDHash,
				optionalQualifiedAttributes, Optional.of(resourceIndexCounter.incrementAndGet()), versionHandler);

		processor.addHashToStatementIndex(hash);
		processor.addStatementToIndex(rel, statementUUIDHash);

		addedRelationships++;

		addResourceProperty(subjectNode, rel, optionalSubjectNodeType, optionalSubjectHash, optionalResourceHash);

		return rel;
	}

	protected Optional<Long> addResourceProperty(final Node subjectNode, final Node objectNode, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<Long> optionalSubjectHash, final Optional<Long> optionalResourceHash) {

		final Optional<Long> finalOptionalResourceHash = processor.determineResourceHash(subjectNode, optionalSubjectNodeType, optionalSubjectHash,
				optionalResourceHash);

		if (!finalOptionalResourceHash.isPresent()) {

			return Optional.absent();
		}

		objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, finalOptionalResourceHash.get());

		return finalOptionalResourceHash;
	}

	protected Optional<Long> addResourceProperty(final Node subjectNode, final Relationship rel, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<Long> optionalSubjectHash, final Optional<Long> optionalResourceHash) {

		final Optional<Long> finalOptionalResourceHash;

		if (optionalResourceHash.isPresent()) {

			finalOptionalResourceHash = optionalResourceHash;
		} else {

			finalOptionalResourceHash = processor.determineResourceHash(subjectNode, optionalSubjectNodeType, optionalSubjectHash, optionalResourceHash);
		}

		if (finalOptionalResourceHash.isPresent()) {

			rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalOptionalResourceHash.get());
		}

		return finalOptionalResourceHash;
	}

	public void addBNode(final Optional<String> optionalNodeId, final Optional<NodeType> optionalNodeType, final Node node) throws DMPGraphException {

		if (!optionalNodeId.isPresent() || !optionalNodeType.isPresent()) {

			throw new DMPGraphException("cannot add bnode, because the node id or node type is not present");
		}

		switch (optionalNodeType.get()) {

			case BNode:

				processor.addNodeToBNodesIndex(optionalNodeId.get(), node);

				break;
		}
	}
}
