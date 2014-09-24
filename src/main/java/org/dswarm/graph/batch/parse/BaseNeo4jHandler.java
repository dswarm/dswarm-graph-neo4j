package org.dswarm.graph.batch.parse;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.Neo4jProcessor;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.Statement;
import org.dswarm.graph.parse.Neo4jHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * @author tgaengler
 */
public abstract class BaseNeo4jHandler implements Neo4jHandler {

	private static final Logger		LOG					= LoggerFactory.getLogger(BaseNeo4jHandler.class);

	protected int totalTriples       = 0;
	protected int addedNodes         = 0;
	protected int addedRelationships = 0;
	protected int sinceLastCommit    = 0;
	protected int i                  = 0;
	protected int literals           = 0;

	protected long tick = System.currentTimeMillis();

	protected String resourceUri;

	protected final Neo4jProcessor processor;

	public BaseNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public Neo4jProcessor getProcessor() {

		return processor;
	}

	@Override
	public void setResourceUri(final String resourceUriArg) {

		resourceUri = resourceUriArg;
	}

	@Override
	public void handleStatement(Statement statement) throws DMPGraphException {

		// utilise r for the resource property

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

		try {

			if (!statement.getOptionalSubjectNodeType().isPresent() || !statement.getOptionalPredicateURI().isPresent()
					|| !statement.getOptionalObjectNodeType().isPresent()) {

				throw new DMPGraphException("cannot handle statement, because no subject node type or predicate uri or object node type is present");
			}

			final NodeType subjectNodeType = statement.getOptionalSubjectNodeType().get();
			final NodeType objectNodeType = statement.getOptionalObjectNodeType().get();

			// Check index for subject
			// TODO: what should we do, if the subject is a resource type?
			final Optional<Long> optionalSubjectNodeId = processor.determineNode(statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectId(), statement.getOptionalSubjectURI(), statement.getOptionalSubjectDataModelURI());
			final Long subjectNodeId;

			if (optionalSubjectNodeId.isPresent()) {

				subjectNodeId = optionalSubjectNodeId.get();
			} else {

				final Map<String, Object> subjectNodeProperties = new HashMap<>();
				subjectNodeId = processor.getBatchInserter().createNode(subjectNodeProperties);

				if (NodeType.Resource.equals(subjectNodeType) || NodeType.TypeResource.equals(subjectNodeType)) {

					// subject is a resource node

					final String subjectURI = statement.getOptionalSubjectURI().get();

					processor.getBatchInserter().setNodeProperty(subjectNodeId, GraphStatics.URI_PROPERTY, subjectURI);
					processor.getBatchInserter().setNodeProperty(subjectNodeId, GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

					processor.handleSubjectDataModel(subjectNodeId, subjectURI, statement.getOptionalSubjectDataModelURI());

					processor.addToResourcesIndex(subjectURI, subjectNodeId);
				} else {

					// subject is a blank node

					// note: can I expect an id here?
					processor.addToBNodesIndex(statement.getOptionalSubjectId().get(), subjectNodeId);
					processor.getBatchInserter().setNodeProperty(subjectNodeId, GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				}

				addedNodes++;
			}

			if (NodeType.Literal.equals(objectNodeType)) {

				handleLiteral(subjectNodeId, statement);
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
				final Long objectNodeId;
				final Optional<String> optionalResourceUri;

				if (optionalObjectNodeId.isPresent()) {

					objectNodeId = optionalObjectNodeId.get();
					optionalResourceUri = Optional.absent();
				} else {

					final Map<String, Object> objectNodeProperties = new HashMap<>();
					objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties);

					if (NodeType.Resource.equals(finalObjectNodeType) || NodeType.TypeResource.equals(finalObjectNodeType)) {

						// object is a resource node

						final String objectURI = statement.getOptionalObjectURI().get();

						processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.URI_PROPERTY, objectURI);

						switch (finalObjectNodeType) {

							case Resource:

								processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.NODETYPE_PROPERTY,
										NodeType.Resource.toString());

								processor.handleObjectDataModel(objectNodeId, statement.getOptionalObjectDataModelURI());

								break;
							case TypeResource:

								processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.NODETYPE_PROPERTY,
										NodeType.TypeResource.toString());
								processor.addLabel(objectNodeId, RDFS.Class.getURI());

								processor.addToResourceTypesIndex(objectURI, objectNodeId);

								break;
						}

						processor.addToResourcesIndex(objectURI, objectNodeId);
						processor.addObjectToResourceWDataModelIndex(objectNodeId, objectURI, statement.getOptionalObjectDataModelURI());
						optionalResourceUri = Optional.absent();
					} else {

						optionalResourceUri = handleBNode(subjectNodeId, statement, objectNodeId, finalOptionalObjectNodeType);
					}

					addedNodes++;
				}

//				final String hash = processor.generateStatementHash(subjectNodeId, statement.getOptionalPredicateURI().get(), objectNodeId,
//						subjectNodeType, finalObjectNodeType);
//
//				final Optional<Long> optionalRelId = processor.getStatement(hash);
//
//				if (!optionalRelId.isPresent()) {

				final String hash = null;

					final Optional<String> finalOptionalResourceUri;

					if (!optionalResourceUri.isPresent()) {

						finalOptionalResourceUri = statement.getOptionalResourceURI();
					} else {

						finalOptionalResourceUri = optionalResourceUri;
					}

					addRelationship(subjectNodeId, statement.getOptionalPredicateURI().get(), objectNodeId, statement.getOptionalSubjectNodeType(),
							statement.getOptionalSubjectURI(), statement.getOptionalStatementUUID(), finalOptionalResourceUri,
							statement.getOptionalQualifiedAttributes(), hash);
				}
//			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 50000 || timeDelta >= 30) { // Commit every 50k operations or every 30 seconds

				sinceLastCommit = totalTriples;

				LOG.debug(totalTriples + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");

				tick = System.currentTimeMillis();
			}

			if(nodeDelta >= 1000000 || timeDelta >= 30) {

				LOG.debug("flush indices");

				processor.flushIndices();
			}
		} catch (final Exception e) {

			final String message = "couldn't finish write TX successfully";

			LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void closeTransaction() {

		LOG.debug("close writing finally");

		processor.clearMaps();
		processor.flushIndices();
	}

	@Override
	public long getCountedStatements() {

		return totalTriples;
	}

	public int getNodesAdded() {

		return addedNodes;
	}

	@Override
	public int getRelationshipsAdded() {

		return addedRelationships;
	}

	public int getCountedLiterals() {

		return literals;
	}

	public Optional<String> handleBNode(final Long subjectNodeId, final Statement statement, final Long objectNodeId,
			final Optional<NodeType> optionalObjectNodeType) throws DMPGraphException {

		if (!optionalObjectNodeType.isPresent()) {

			throw new DMPGraphException("there is no object node type present");
		}

		final Optional<String> optionalResourceUri;
		// object is a blank node

		processor.addToBNodesIndex(statement.getOptionalObjectId().get(), objectNodeId);

		final NodeType objectNodeType = optionalObjectNodeType.get();
		processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.NODETYPE_PROPERTY, objectNodeType.toString());

		if (!NodeType.TypeBNode.equals(objectNodeType)) {

			optionalResourceUri = addResourceProperty(subjectNodeId, objectNodeId, statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectURI(), statement.getOptionalResourceURI());
		} else {

			processor.addLabel(objectNodeId, RDFS.Class.getURI());
			optionalResourceUri = Optional.absent();
		}

		return optionalResourceUri;
	}

	public void handleLiteral(final Long subjectNodeId, final Statement statement) throws DMPGraphException {

		final String hash = processor.generateStatementHash(subjectNodeId, statement.getOptionalPredicateURI().get(), statement
				.getOptionalObjectValue().get(), statement.getOptionalSubjectNodeType().get(), statement.getOptionalObjectNodeType().get());

		final Optional<Long> optionalRelId = processor.getStatement(hash);

		if (!optionalRelId.isPresent()) {

			literals++;

			final Map<String, Object> objectNodeProperties = new HashMap<>();
			final Long objectNodeId = processor.getBatchInserter().createNode(objectNodeProperties);
			processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.VALUE_PROPERTY, statement.getOptionalObjectValue().get());
			processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
			processor.addToValueIndex(statement.getOptionalObjectValue().get(), objectNodeId);

			final Optional<String> optionalResourceUri = addResourceProperty(subjectNodeId, objectNodeId, statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectURI(), statement.getOptionalResourceURI());

			addedNodes++;

			addRelationship(subjectNodeId, statement.getOptionalPredicateURI().get(), objectNodeId, statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectURI(), statement.getOptionalStatementUUID(), optionalResourceUri,
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
	public Long addRelationship(final Long subjectNodeId, final String predicateURI, final Long objectNodeId,
			final Optional<NodeType> optionalSubjectNodeType, final Optional<String> optionalSubjectURI,
			final Optional<String> optionalStatementUUID, final Optional<String> optionalResourceUri,
			final Optional<Map<String, Object>> optionalQualifiedAttributes, final String hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (optionalStatementUUID.isPresent()) {

			finalStatementUUID = optionalStatementUUID.get();
		} else {

			finalStatementUUID = UUID.randomUUID().toString();
		}

		final Long relId = processor.prepareRelationship(subjectNodeId, predicateURI, objectNodeId, finalStatementUUID,
				optionalQualifiedAttributes);

//		processor.addToStatementIndex(hash, relId);
//		processor.addStatementToIndex(relId, finalStatementUUID);

		addedRelationships++;

		addResourcePropertyToRelationship(subjectNodeId, relId, optionalSubjectNodeType, optionalSubjectURI, optionalResourceUri);

		return relId;
	}

	protected Optional<String> addResourceProperty(final Long subjectNodeId, final Long objectNodeId, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final Optional<String> optionalResourceUri = processor.determineResourceUri(subjectNodeId, optionalSubjectNodeType, optionalSubjectURI,
				optionalResourceURI);

		if (!optionalResourceUri.isPresent()) {

			return Optional.absent();
		}

		processor.getBatchInserter().setNodeProperty(objectNodeId, GraphStatics.RESOURCE_PROPERTY, optionalResourceUri.get());

		return optionalResourceUri;
	}

	protected Optional<String> addResourcePropertyToRelationship(final Long subjectNodeId, final Long relId, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final Optional<String> finalOptionalResourceUri;

		if (optionalResourceURI.isPresent()) {

			finalOptionalResourceUri = optionalResourceURI;
		} else {

			finalOptionalResourceUri = processor.determineResourceUri(subjectNodeId, optionalSubjectNodeType, optionalSubjectURI, optionalResourceURI);
		}

		if(finalOptionalResourceUri.isPresent()) {

			processor.getBatchInserter().setRelationshipProperty(relId, GraphStatics.RESOURCE_PROPERTY, finalOptionalResourceUri.get());
		}

		return finalOptionalResourceUri;
	}

	public void addBNode(final Optional<String> optionalNodeId, final Optional<NodeType> optionalNodeType, final Long nodeId) throws DMPGraphException {

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
