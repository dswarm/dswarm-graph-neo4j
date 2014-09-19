package org.dswarm.graph.gdm;

import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.gdm.utils.NodeTypeUtils;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

/**
 * @author tgaengler
 */
public abstract class GDMNeo4jProcessor {

	private static final Logger		LOG	= LoggerFactory.getLogger(GDMNeo4jProcessor.class);

	protected final Neo4jProcessor	processor;

	public GDMNeo4jProcessor(final Neo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public Neo4jProcessor getProcessor() {

		return processor;
	}

	public Optional<Node> determineNode(final org.dswarm.graph.json.Node resource, final boolean isType) {

		final Optional<org.dswarm.graph.json.Node> optionalResource = Optional.fromNullable(resource);
		final Optional<NodeType> optionalResourceNodeType = NodeTypeUtils.getNodeType(optionalResource, Optional.of(isType));

		final Optional<String> optionalResourceId;
		final Optional<String> optionalResourceUri;
		final Optional<String> optionalDataModelUri;

		if (optionalResource.isPresent()) {

			if (resource.getId() != null) {

				optionalResourceId = Optional.of("" + resource.getId());
			} else {

				optionalResourceId = Optional.absent();
			}

			if (optionalResourceNodeType.isPresent()
					&& (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get()))) {

				final ResourceNode resourceResourceNode = (ResourceNode) resource;

				optionalResourceUri = Optional.fromNullable(resourceResourceNode.getUri());
				optionalDataModelUri = Optional.fromNullable(resourceResourceNode.getDataModel());
			} else {

				optionalResourceUri = Optional.absent();
				optionalDataModelUri = Optional.absent();
			}
		} else {

			optionalResourceId = Optional.absent();
			optionalResourceUri = Optional.absent();
			optionalDataModelUri = Optional.absent();
		}

		return processor.determineNode(optionalResourceNodeType, optionalResourceId, optionalResourceUri, optionalDataModelUri);
	}

	public Optional<String> determineResourceUri(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Resource resource) {

		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeType(Optional.fromNullable(subject));

		final Optional<String> optionalSubjectURI;

		if (optionalSubjectNodeType.isPresent() && NodeType.Resource.equals(optionalSubjectNodeType.get())) {

			optionalSubjectURI = Optional.fromNullable(((ResourceNode) subject).getUri());
		} else {

			optionalSubjectURI = Optional.absent();
		}

		final Optional<String> optionalResourceURI;

		if (resource != null) {

			optionalResourceURI = Optional.fromNullable(resource.getUri());
		} else {

			optionalResourceURI = Optional.absent();
		}

		return processor.determineResourceUri(subjectNode, optionalSubjectNodeType, optionalSubjectURI, optionalResourceURI);
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.fromNullable(subjectNodeType));
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.fromNullable(objectNodeType));
		final Optional<String> optionalSubjectIdentifier = processor.getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = processor.getIdentifier(objectNode, optionalObjectNodeType);

		return processor.generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final String objectValue,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.fromNullable(subjectNodeType));
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.fromNullable(objectNodeType));
		final Optional<String> optionalSubjectIdentifier = processor.getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = Optional.fromNullable(objectValue);

		return processor.generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index, final VersionHandler versionHandler) {

		final String predicateURI = statement.getPredicate().getUri();

		final Map<String, Object> qualifiedAttributes = Maps.newHashMap();

		if (statement.getOrder() != null) {

			qualifiedAttributes.put(GraphStatics.ORDER_PROPERTY, statement.getOrder());
		}

		if (statement.getEvidence() != null) {

			qualifiedAttributes.put(GraphStatics.EVIDENCE_PROPERTY, statement.getEvidence());
		}

		if (statement.getConfidence() != null) {

			qualifiedAttributes.put(GraphStatics.CONFIDENCE_PROPERTY, statement.getConfidence());
		}

		return processor.prepareRelationship(subjectNode, predicateURI, objectNode, statementUUID, qualifiedAttributes, index, versionHandler);
	}
}
