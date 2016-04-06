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
package org.dswarm.graph.gdm;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.gdm.utils.NodeTypeUtils;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.StatementBuilder;

/**
 * @author tgaengler
 */
public abstract class GDMNeo4jProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(GDMNeo4jProcessor.class);

	protected final BasicNeo4jProcessor processor;

	public GDMNeo4jProcessor(final BasicNeo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public BasicNeo4jProcessor getProcessor() {

		return processor;
	}

	public StatementBuilder determineNode(final org.dswarm.graph.json.Node resource, final StatementBuilder statementBuilder,
	                                      final boolean forSubject) {

		final Optional<org.dswarm.graph.json.Node> optionalResource = Optional.ofNullable(resource);
		final Optional<NodeType> optionalResourceNodeType = NodeTypeUtils.getNodeType(optionalResource);

		final Optional<String> optionalResourceId;
		final Optional<String> optionalResourceUri;
		final Optional<String> optionalDataModelUri;
		final Optional<String> optionalResourceValue;

		if (optionalResource.isPresent()) {

			if (resource.getId() != null) {

				optionalResourceId = Optional.of("" + resource.getId());
			} else {

				optionalResourceId = Optional.empty();
			}

			if (optionalResourceNodeType.isPresent()) {

				if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

					final ResourceNode resourceResourceNode = (ResourceNode) resource;

					optionalResourceUri = Optional.ofNullable(resourceResourceNode.getUri());
					optionalDataModelUri = Optional.ofNullable(resourceResourceNode.getDataModel());
					optionalResourceValue = Optional.empty();
				} else if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

					optionalResourceValue = Optional.ofNullable(((LiteralNode) resource).getValue());
					optionalResourceUri = Optional.empty();
					optionalDataModelUri = Optional.empty();
				} else {

					optionalResourceUri = Optional.empty();
					optionalDataModelUri = Optional.empty();
					optionalResourceValue = Optional.empty();
				}
			} else {

				optionalResourceUri = Optional.empty();
				optionalDataModelUri = Optional.empty();
				optionalResourceValue = Optional.empty();
			}
		} else {

			optionalResourceId = Optional.empty();
			optionalResourceUri = Optional.empty();
			optionalDataModelUri = Optional.empty();
			optionalResourceValue = Optional.empty();
		}

		if (forSubject) {

			statementBuilder.setOptionalSubjectId(optionalResourceId);
			statementBuilder.setOptionalSubjectURI(optionalResourceUri);
			statementBuilder.setOptionalSubjectDataModelURI(optionalDataModelUri);
		} else {

			statementBuilder.setOptionalObjectId(optionalResourceId);
			statementBuilder.setOptionalObjectURI(optionalResourceUri);
			statementBuilder.setOptionalObjectDataModelURI(optionalDataModelUri);
			statementBuilder.setOptionalObjectValue(optionalResourceValue);
		}

		return statementBuilder;
	}

	public Optional<Long> determineResourceHash(final org.dswarm.graph.json.Node subject, final long resourceHash) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeType(Optional.ofNullable(subject));

		final Optional<Long> optionalSubjectHash;

		if (optionalSubjectNodeType.isPresent()
				&& (NodeType.Resource.equals(optionalSubjectNodeType.get()) || NodeType.TypeResource.equals(optionalSubjectNodeType.get()))) {

			final ResourceNode subjectNode = (ResourceNode) subject;
			final String subjectUri = processor.createPrefixedURI(subjectNode.getUri());

			final Optional<String> optionalDataModelURI;

			if (subjectNode.getDataModel() != null) {

				final String dataModelUri = processor.createPrefixedURI(subjectNode.getDataModel());
				optionalDataModelURI = Optional.of(dataModelUri);
			} else {

				optionalDataModelURI = Optional.empty();
			}

			final long subjectUriDataModelUriHash = processor.generateResourceHash(subjectUri, optionalDataModelURI);
			optionalSubjectHash = Optional.ofNullable(subjectUriDataModelUriHash);
		} else {

			optionalSubjectHash = Optional.empty();
		}

		final Optional<Long> optionalResourceHash = Optional.ofNullable(resourceHash);

		return processor.determineResourceHash(optionalSubjectNodeType, optionalSubjectHash, optionalResourceHash);
	}

	public long generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode,
	                                  final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.ofNullable(subjectNodeType));
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.ofNullable(objectNodeType));
		final Optional<String> optionalSubjectIdentifier = processor.getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = processor.getIdentifier(objectNode, optionalObjectNodeType);

		return processor.generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public long generateStatementHash(final Node subjectNode, final String predicateName, final String objectValue,
	                                  final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.ofNullable(subjectNodeType));
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeTypeByGDMNodeType(Optional.ofNullable(objectNodeType));
		final Optional<String> optionalSubjectIdentifier = processor.getIdentifier(subjectNode, optionalSubjectNodeType);
		final Optional<String> optionalObjectIdentifier = Optional.ofNullable(objectValue);

		return processor.generateStatementHash(predicateName, optionalSubjectNodeType, optionalObjectNodeType, optionalSubjectIdentifier,
				optionalObjectIdentifier);
	}

	public Map<String, Object> getQualifiedAttributes(final Statement statement) {

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

		return qualifiedAttributes;
	}
}
