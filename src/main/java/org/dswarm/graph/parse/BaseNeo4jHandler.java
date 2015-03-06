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

import org.apache.commons.lang.NotImplementedException;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.Statement;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * @author tgaengler
 */
public abstract class BaseNeo4jHandler implements Neo4jHandler, Neo4jUpdateHandler {

	private static final Logger		LOG					= LoggerFactory.getLogger(BaseNeo4jHandler.class);

	protected int					totalTriples		= 0;
	protected int					addedNodes			= 0;
	protected int					addedRelationships	= 0;
	protected int					sinceLastCommit		= 0;
	protected int					i					= 0;
	protected int					literals			= 0;

	protected long					tick				= System.currentTimeMillis();

	protected String				resourceUri;

	// TODO: init
	protected VersionHandler		versionHandler		= null;

	protected final Neo4jProcessor	processor;

	public BaseNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;

		init();
	}

	public Neo4jProcessor getProcessor() {

		return processor;
	}

	@Override
	public void setResourceUri(final String resourceUriArg) {

		resourceUri = resourceUriArg;
	}

	@Override
	public VersionHandler getVersionHandler() {

		return versionHandler;
	}

	@Override
	public void handleStatement(Statement statement) throws DMPGraphException {

		// utilise r for the resource property

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

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
			final Optional<Node> optionalSubjectNode = processor.determineNode(statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectId(), statement.getOptionalSubjectURI(), statement.getOptionalSubjectDataModelURI());
			final Node subjectNode;

			if (optionalSubjectNode.isPresent()) {

				subjectNode = optionalSubjectNode.get();
			} else {

				subjectNode = processor.getDatabase().createNode();

				if (NodeType.Resource.equals(subjectNodeType) || NodeType.TypeResource.equals(subjectNodeType)) {

					// subject is a resource node

					final String subjectURI = statement.getOptionalSubjectURI().get();

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subjectURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

					if (resourceUri != null && resourceUri.equals(subjectURI)) {

						versionHandler.setLatestVersion(statement.getOptionalSubjectDataModelURI());
					}

					processor.handleSubjectDataModel(subjectNode, subjectURI, statement.getOptionalSubjectDataModelURI());

					processor.addNodeToResourcesIndex(subjectURI, subjectNode);
				} else {

					// subject is a blank node

					// note: can I expect an id here?
					processor.getBNodesIndex().put(statement.getOptionalSubjectId().get(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				}

				addedNodes++;
			}

			if (NodeType.Literal.equals(objectNodeType)) {

				handleLiteral(subjectNode, statement);
			} else { // must be Resource
				// Make sure object exists

				boolean isType = false;

				// add Label if this is a type entry
				if (statement.getOptionalPredicateURI().get().equals(RDF.type.getURI())) {

					processor.addLabel(subjectNode, statement.getOptionalObjectURI().get());

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
				final Optional<Node> optionalObjectNode = processor.determineNode(finalOptionalObjectNodeType, statement.getOptionalObjectId(),
						statement.getOptionalObjectURI(), statement.getOptionalObjectDataModelURI());
				final Node objectNode;
				final Optional<String> optionalResourceUri;

				if (optionalObjectNode.isPresent()) {

					objectNode = optionalObjectNode.get();
					optionalResourceUri = Optional.absent();
				} else {

					objectNode = processor.getDatabase().createNode();

					if (NodeType.Resource.equals(finalObjectNodeType) || NodeType.TypeResource.equals(finalObjectNodeType)) {

						// object is a resource node

						final String objectURI = statement.getOptionalObjectURI().get();

						objectNode.setProperty(GraphStatics.URI_PROPERTY, objectURI);

						switch (finalObjectNodeType) {

							case Resource:

								objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

								processor.handleObjectDataModel(objectNode, statement.getOptionalObjectDataModelURI());

								break;
							case TypeResource:

								objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
								processor.addLabel(objectNode, RDFS.Class.getURI());

								processor.addNodeToResourceTypesIndex(objectURI, objectNode);

								break;
						}

						processor.addObjectToResourceWDataModelIndex(objectNode, objectURI, statement.getOptionalObjectDataModelURI());
						optionalResourceUri = Optional.absent();
					} else {

						optionalResourceUri = handleBNode(subjectNode, statement, objectNode, finalOptionalObjectNodeType);
					}

					addedNodes++;
				}

				final long hash = processor.generateStatementHash(subjectNode, statement.getOptionalPredicateURI().get(), objectNode,
						subjectNodeType, finalObjectNodeType);

				final Relationship rel = processor.getStatement(hash);

				if (rel == null) {

					final Optional<String> finalOptionalResourceUri;

					if (!optionalResourceUri.isPresent()) {

						finalOptionalResourceUri = statement.getOptionalResourceURI();
					} else {

						finalOptionalResourceUri = optionalResourceUri;
					}

					addRelationship(subjectNode, statement.getOptionalPredicateURI().get(), objectNode, statement.getOptionalSubjectNodeType(),
							statement.getOptionalSubjectURI(), statement.getOptionalStatementUUID(), finalOptionalResourceUri,
							statement.getOptionalQualifiedAttributes(), hash);
				}
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 50000 || timeDelta >= 30) { // Commit every 50k operations or every 30 seconds

				processor.renewTx();

				sinceLastCommit = totalTriples;

				LOG.debug(totalTriples + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");

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

			final Relationship rel = getRelationship(uuid);

			rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getLatestVersion());

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
	public void closeTransaction() {

		LOG.debug("close write TX finally");

		processor.succeedTx();
		processor.clearMaps();
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

	protected abstract void init() throws DMPGraphException;

	public abstract Relationship getRelationship(final String uuid);

	public Optional<String> handleBNode(final Node subjectNode, final Statement statement, final Node objectNode,
			final Optional<NodeType> optionalObjectNodeType) throws DMPGraphException {

		if (!optionalObjectNodeType.isPresent()) {

			throw new DMPGraphException("there is no object node type present");
		}

		final Optional<String> optionalResourceUri;
		// object is a blank node

		processor.getBNodesIndex().put(statement.getOptionalObjectId().get(), objectNode);

		final NodeType objectNodeType = optionalObjectNodeType.get();
		objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, objectNodeType.toString());

		if (!NodeType.TypeBNode.equals(objectNodeType)) {

			optionalResourceUri = addResourceProperty(subjectNode, objectNode, statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectURI(), statement.getOptionalResourceURI());
		} else {

			processor.addLabel(objectNode, RDFS.Class.getURI());
			optionalResourceUri = Optional.absent();
		}

		return optionalResourceUri;
	}

	public void handleLiteral(final Node subjectNode, final Statement statement) throws DMPGraphException {

		final long hash = processor.generateStatementHash(subjectNode, statement.getOptionalPredicateURI().get(), statement.getOptionalObjectValue()
				.get(), statement.getOptionalSubjectNodeType().get(), statement.getOptionalObjectNodeType().get());

		final Relationship rel = processor.getStatement(hash);

		if (rel == null) {

			literals++;

			final Node objectNode = processor.getDatabase().createNode();
			objectNode.setProperty(GraphStatics.VALUE_PROPERTY, statement.getOptionalObjectValue().get());
			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
			processor.getValueIndex().add(objectNode, GraphStatics.VALUE, statement.getOptionalObjectValue().get());

			final Optional<String> optionalResourceUri = addResourceProperty(subjectNode, objectNode, statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectURI(), statement.getOptionalResourceURI());

			addedNodes++;

			addRelationship(subjectNode, statement.getOptionalPredicateURI().get(), objectNode, statement.getOptionalSubjectNodeType(),
					statement.getOptionalSubjectURI(), statement.getOptionalStatementUUID(), optionalResourceUri,
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
	 * @param optionalSubjectURI
	 * @param optionalStatementUUID
	 * @param optionalResourceUri
	 * @param optionalQualifiedAttributes
	 * @param hash
	 * @return
	 * @throws DMPGraphException
	 */
	public Relationship addRelationship(final Node subjectNode, final String predicateURI, final Node objectNode,
			final Optional<NodeType> optionalSubjectNodeType, final Optional<String> optionalSubjectURI,
			final Optional<String> optionalStatementUUID, final Optional<String> optionalResourceUri,
			final Optional<Map<String, Object>> optionalQualifiedAttributes, final long hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (optionalStatementUUID.isPresent()) {

			finalStatementUUID = optionalStatementUUID.get();
		} else {

			finalStatementUUID = UUID.randomUUID().toString();
		}

		final Relationship rel = processor.prepareRelationship(subjectNode, predicateURI, objectNode, finalStatementUUID,
				optionalQualifiedAttributes, versionHandler);

		processor.getStatementHashesIndex().acquireUsing(hash, rel.getId());
		processor.addStatementToIndex(rel, finalStatementUUID);

		addedRelationships++;

		addResourceProperty(subjectNode, rel, optionalSubjectNodeType, optionalSubjectURI, optionalResourceUri);

		return rel;
	}

	protected Optional<String> addResourceProperty(final Node subjectNode, final Node objectNode, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final Optional<String> optionalResourceUri = processor.determineResourceUri(subjectNode, optionalSubjectNodeType, optionalSubjectURI,
				optionalResourceURI);

		if (!optionalResourceUri.isPresent()) {

			return Optional.absent();
		}

		objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, optionalResourceUri.get());

		return optionalResourceUri;
	}

	protected Optional<String> addResourceProperty(final Node subjectNode, final Relationship rel, final Optional<NodeType> optionalSubjectNodeType,
			final Optional<String> optionalSubjectURI, final Optional<String> optionalResourceURI) {

		final Optional<String> finalOptionalResourceUri;

		if (optionalResourceURI.isPresent()) {

			finalOptionalResourceUri = optionalResourceURI;
		} else {

			finalOptionalResourceUri = processor.determineResourceUri(subjectNode, optionalSubjectNodeType, optionalSubjectURI, optionalResourceURI);
		}

		if (finalOptionalResourceUri.isPresent()) {

			rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalOptionalResourceUri.get());
		}

		return finalOptionalResourceUri;
	}

	public void addBNode(final Optional<String> optionalNodeId, final Optional<NodeType> optionalNodeType, final Node node) throws DMPGraphException {

		if (!optionalNodeId.isPresent() || !optionalNodeType.isPresent()) {

			throw new DMPGraphException("cannot add bnode, because the node id or node type is not present");
		}

		switch (optionalNodeType.get()) {

			case BNode:

				processor.getBNodesIndex().put(optionalNodeId.get(), node);

				break;
		}
	}
}
