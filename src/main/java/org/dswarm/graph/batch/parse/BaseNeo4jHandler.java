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
package org.dswarm.graph.batch.parse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.BatchNeo4jProcessor;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.Statement;
import org.dswarm.graph.parse.Neo4jHandler;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * @author tgaengler
 */
public abstract class BaseNeo4jHandler implements Neo4jHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BaseNeo4jHandler.class);

	private static final int TX_CHUNK_SIZE = 200000;
	private static final int TX_TIME_DELTA = 30;

	protected int totalTriples       = 0;
	protected int addedNodes         = 0;
	protected int addedRelationships = 0;
	protected int sinceLastCommit    = 0;
	protected int i                  = 0;
	protected int literals           = 0;

	protected long tick = System.currentTimeMillis();

	protected String resourceUri;
	protected long   resourceHash;

	protected final BatchNeo4jProcessor processor;

	protected static final Label rdfsClassLabel = DynamicLabel.label(RDFS.Class.getURI());

	public BaseNeo4jHandler(final BatchNeo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public Neo4jProcessor getProcessor() {

		return processor;
	}

	@Override
	public void setResourceUri(final String resourceUriArg) {

		resourceUri = resourceUriArg;
	}

	@Override public void setResourceHash(final long resourceHashArg) {

		resourceHash = resourceHashArg;
	}

	@Override
	public void handleStatement(final Statement statement) throws DMPGraphException {

		// utilise r for the resource property

		i++;

		try {

			if (!statement.getOptionalSubjectNodeType().isPresent() || !statement.getOptionalPredicateURI().isPresent()
					|| !statement.getOptionalObjectNodeType().isPresent()) {

				throw new DMPGraphException("cannot handle statement, because no subject node type or predicate uri or object node type is present");
			}

			final NodeType subjectNodeType = statement.getOptionalSubjectNodeType().get();
			final NodeType objectNodeType = statement.getOptionalObjectNodeType().get();

			final Optional<String> optionalSubjectURI = statement.getOptionalSubjectURI();

			final Optional<Long> optionalSubjectUriDataModelUriHash;

			if (optionalSubjectURI.isPresent()) {

				optionalSubjectUriDataModelUriHash = Optional
						.of(processor.generateResourceHash(optionalSubjectURI.get(), statement.getOptionalSubjectDataModelURI()));
			} else {

				optionalSubjectUriDataModelUriHash = Optional.absent();
			}

			// Check index for subject
			// TODO: what should we do, if the subject is a resource type?
			final Optional<Long> optionalSubjectNodeId = processor.determineNode(statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectId(), statement.getOptionalSubjectURI(), statement.getOptionalSubjectDataModelURI());
			final long subjectNodeId;

			if (optionalSubjectNodeId.isPresent()) {

				subjectNodeId = optionalSubjectNodeId.get();
			} else {

				final Map<String, Object> subjectNodeProperties = new HashMap<>();

				if (NodeType.Resource.equals(subjectNodeType) || NodeType.TypeResource.equals(subjectNodeType)) {

					// subject is a resource node

					final String subjectURI = statement.getOptionalSubjectURI().get();

					subjectNodeProperties.put(GraphStatics.URI_PROPERTY, subjectURI);
					subjectNodeProperties.put(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

					processor.handleSubjectDataModel(subjectNodeProperties, subjectURI, statement.getOptionalSubjectDataModelURI());

					subjectNodeId = processor.getBatchInserter().createNode(subjectNodeProperties);

					processor.addToResourcesIndex(subjectURI, subjectNodeId);
					processor.addObjectToResourceWDataModelIndex(subjectNodeId, subjectURI, statement.getOptionalSubjectDataModelURI());
				} else {

					// subject is a blank node

					// note: can I expect an id here?
					subjectNodeProperties.put(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());

					subjectNodeId = processor.getBatchInserter().createNode(subjectNodeProperties);

					processor.addToBNodesIndex(statement.getOptionalSubjectId().get(), subjectNodeId);
				}

				addedNodes++;
			}

			if (NodeType.Literal.equals(objectNodeType)) {

				handleLiteral(subjectNodeId, statement, optionalSubjectUriDataModelUriHash);
			} else { // must be Resource
				// Make sure object exists

				boolean isType = false;

				// add Label if this is a type entry
				if (statement.getOptionalPredicateURI().get().equals(RDF.type.getURI())) {

					processor.addLabel(subjectNodeId, statement.getOptionalObjectURI().get());

					isType = true;
				}

				final NodeType finalObjectNodeType;

				if (!isType) {

					finalObjectNodeType = objectNodeType;
				} else {

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

				final Optional<NodeType> finalOptionalObjectNodeType = Optional.of(finalObjectNodeType);

				// Check index for object
				final Optional<Long> optionalObjectNodeId = processor.determineNode(finalOptionalObjectNodeType, statement.getOptionalObjectId(),
						statement.getOptionalObjectURI(), statement.getOptionalObjectDataModelURI());
				final long objectNodeId;
				final Optional<Long> optionalResourceHash;

				if (optionalObjectNodeId.isPresent()) {

					objectNodeId = optionalObjectNodeId.get();
					optionalResourceHash = Optional.absent();
				} else {

					final Map<String, Object> objectNodeProperties = new HashMap<>();

					if (NodeType.Resource.equals(finalObjectNodeType) || NodeType.TypeResource.equals(finalObjectNodeType)) {

						// object is a resource node

						final String objectURI = statement.getOptionalObjectURI().get();

						objectNodeProperties.put(GraphStatics.URI_PROPERTY, objectURI);

						switch (finalObjectNodeType) {

							case Resource:

								objectNodeProperties.put(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

								processor.handleObjectDataModel(objectNodeProperties, statement.getOptionalObjectDataModelURI());

								objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties);

								break;
							case TypeResource:

								objectNodeProperties.put(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());

								objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties, rdfsClassLabel);

								processor.addToResourceTypesIndex(objectURI, objectNodeId);

								break;
							default:

								throw new DMPGraphException("object must be a resource or a type resource at this moment");
						}

						processor.addToResourcesIndex(objectURI, objectNodeId);
						processor.addObjectToResourceWDataModelIndex(objectNodeId, objectURI, statement.getOptionalObjectDataModelURI());
						optionalResourceHash = Optional.absent();
					} else {

						final Pair<Long, Optional<Long>> result = handleBNode(subjectNodeId, statement, objectNodeProperties,
								finalOptionalObjectNodeType, optionalSubjectUriDataModelUriHash);
						objectNodeId = result.first();
						optionalResourceHash = result.other();
					}

					addedNodes++;
				}

				final long hash = processor.generateStatementHash(subjectNodeId, statement.getOptionalPredicateURI().get(), objectNodeId,
						subjectNodeType, finalObjectNodeType);

				final boolean statementExists = processor.checkStatementExists(hash);

				if (!statementExists) {

					final Optional<Long> finalOptionalResourceHash;

					if (!optionalResourceHash.isPresent()) {

						finalOptionalResourceHash = statement.getOptionalResourceHash();
					} else {

						finalOptionalResourceHash = optionalResourceHash;
					}

					addRelationship(subjectNodeId, statement.getOptionalPredicateURI().get(), objectNodeId, statement.getOptionalSubjectNodeType(),
							optionalSubjectUriDataModelUriHash, statement.getOptionalStatementUUID(), finalOptionalResourceHash,
							statement.getOptionalQualifiedAttributes(), hash);
				}
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= TX_CHUNK_SIZE || timeDelta >= TX_TIME_DELTA) { // "commit" every 200k operations or every 30 seconds

				sinceLastCommit = totalTriples;
				final double duration = (double) nodeDelta / timeDelta;

				BaseNeo4jHandler.LOG.debug("{} triples @ ~{} triples/second.", totalTriples, duration);

				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			final String message = "couldn't finish write \"TX\" successfully";

			BaseNeo4jHandler.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void closeTransaction() throws DMPGraphException {

		BaseNeo4jHandler.LOG.debug("close writing finally");

		processor.clearMaps();
		processor.flushIndices();
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

	public Pair<Long, Optional<Long>> handleBNode(final long subjectNodeId, final Statement statement,
			final Map<String, Object> objectNodeProperties, final Optional<NodeType> optionalObjectNodeType, final Optional<Long> optionalSubjectHash)
			throws DMPGraphException {

		if (!optionalObjectNodeType.isPresent()) {

			throw new DMPGraphException("there is no object node type present");
		}

		final Optional<Long> optionalResourceHash;
		// object is a blank node

		final NodeType objectNodeType = optionalObjectNodeType.get();
		objectNodeProperties.put(GraphStatics.NODETYPE_PROPERTY, objectNodeType.toString());

		final Optional<Label> optionalLabel;

		if (!NodeType.TypeBNode.equals(objectNodeType)) {

			optionalResourceHash = addResourceProperty(subjectNodeId, objectNodeProperties, statement.getOptionalSubjectNodeType(),
					optionalSubjectHash, statement.getOptionalResourceHash());

			optionalLabel = Optional.absent();
		} else {

			optionalLabel = Optional.of(rdfsClassLabel);
			optionalResourceHash = Optional.absent();
		}

		final long objectNodeId;

		if (!optionalLabel.isPresent()) {

			objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties);
		} else {

			objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties, optionalLabel.get());
		}

		processor.addToBNodesIndex(statement.getOptionalObjectId().get(), objectNodeId);

		return Pair.of(objectNodeId, optionalResourceHash);
	}

	public void handleLiteral(final long subjectNodeId, final Statement statement, final Optional<Long> optionalSubjectHash)
			throws DMPGraphException {

		final long hash = processor.generateStatementHash(subjectNodeId, statement);

		final boolean statementExists = processor.checkStatementExists(hash);

		if (!statementExists) {

			literals++;

			final Map<String, Object> objectNodeProperties = new HashMap<>();
			objectNodeProperties.put(GraphStatics.VALUE_PROPERTY, statement.getOptionalObjectValue().get());
			objectNodeProperties.put(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());

			final Optional<Long> optionalResourceHash = addResourceProperty(subjectNodeId, objectNodeProperties,
					statement.getOptionalSubjectNodeType(), optionalSubjectHash, statement.getOptionalResourceHash());

			final long objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties);

			processor.addToValueIndex(statement.getOptionalObjectValue().get(), objectNodeId);

			addedNodes++;

			addRelationship(subjectNodeId, statement.getOptionalPredicateURI().get(), objectNodeId, statement.getOptionalSubjectNodeType(),
					optionalSubjectHash, statement.getOptionalStatementUUID(), optionalResourceHash,
					statement.getOptionalQualifiedAttributes(), hash);
		}
	}

	/**
	 * TODO: refactor this to protected
	 *
	 * @param subjectNodeId
	 * @param predicateURI
	 * @param objectNodeId
	 * @param optionalSubjectNodeType
	 * @param optionalSubjectURI
	 * @param optionalStatementUUID
	 * @param optionalResourceUri
	 * @param optionalQualifiedAttributes
	 * @param hash
	 * @return
	 * @throws org.dswarm.graph.DMPGraphException
	 */
	public long addRelationship(final long subjectNodeId, final String predicateURI, final long objectNodeId,
			final Optional<NodeType> optionalSubjectNodeType, final Optional<Long> optionalSubjectURI,
			final Optional<String> optionalStatementUUID, final Optional<Long> optionalResourceUri,
			final Optional<Map<String, Object>> optionalQualifiedAttributes, final long hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (optionalStatementUUID.isPresent()) {

			finalStatementUUID = optionalStatementUUID.get();
		} else {

			finalStatementUUID = UUID.randomUUID().toString();
		}

		final Map<String, Object> relProperties = processor.prepareRelationship(finalStatementUUID, optionalQualifiedAttributes);

		addResourcePropertyToRelationship(subjectNodeId, relProperties, optionalSubjectNodeType, optionalSubjectURI, optionalResourceUri);

		final RelationshipType relType = DynamicRelationshipType.withName(predicateURI);

		final long relId = processor.getBatchInserter().createRelationship(subjectNodeId, objectNodeId, relType, relProperties);

		// TODO: for now we only keey the hash
		processor.addToStatementIndex(hash);
		processor.addStatementToIndex(relId, finalStatementUUID);

		addedRelationships++;

		return relId;
	}

	protected Optional<Long> addResourceProperty(final long subjectNodeId, final Map<String, Object> objectProperties,
			final Optional<NodeType> optionalSubjectNodeType, final Optional<Long> optionalSubjectHash, final Optional<Long> optionalResourceHash) {

		final Optional<Long> finalOptionalResourceHash = processor.determineResourceHash(subjectNodeId, optionalSubjectNodeType, optionalSubjectHash,
				optionalResourceHash);

		if (!finalOptionalResourceHash.isPresent()) {

			return Optional.absent();
		}

		objectProperties.put(GraphStatics.RESOURCE_PROPERTY, finalOptionalResourceHash.get());

		return finalOptionalResourceHash;
	}

	protected Optional<Long> addResourcePropertyToRelationship(final long subjectNodeId, final Map<String, Object> relProperties,
			final Optional<NodeType> optionalSubjectNodeType, final Optional<Long> optionalSubjectHash, final Optional<Long> optionalResourceHash) {

		final Optional<Long> finalOptionalResourceHash;

		if (optionalResourceHash.isPresent()) {

			finalOptionalResourceHash = optionalResourceHash;
		} else {

			finalOptionalResourceHash = processor
					.determineResourceHash(subjectNodeId, optionalSubjectNodeType, optionalSubjectHash, optionalResourceHash);
		}

		if (finalOptionalResourceHash.isPresent()) {

			relProperties.put(GraphStatics.RESOURCE_PROPERTY, finalOptionalResourceHash.get());
		}

		return finalOptionalResourceHash;
	}

	public void addBNode(final Optional<String> optionalNodeId, final Optional<NodeType> optionalNodeType, final long nodeId)
			throws DMPGraphException {

		if (!optionalNodeId.isPresent() || !optionalNodeType.isPresent()) {

			throw new DMPGraphException("cannot add bnode, because the node id or node type is not present");
		}

		switch (optionalNodeType.get()) {

			case BNode:

				processor.addToBNodesIndex(optionalNodeId.get(), nodeId);

				break;
		}
	}
}
