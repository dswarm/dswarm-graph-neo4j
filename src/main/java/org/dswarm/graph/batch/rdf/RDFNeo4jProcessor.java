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
package org.dswarm.graph.batch.rdf;

import java.util.Optional;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.BatchNeo4jProcessor;
import org.dswarm.graph.model.StatementBuilder;
import org.dswarm.graph.rdf.utils.NodeTypeUtils;

/**
 * @author tgaengler
 */
public abstract class RDFNeo4jProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(RDFNeo4jProcessor.class);

	protected final BatchNeo4jProcessor processor;

	public RDFNeo4jProcessor(final BatchNeo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public BatchNeo4jProcessor getProcessor() {

		return processor;
	}

	public StatementBuilder determineNode(final RDFNode resource, final StatementBuilder statementBuilder, final boolean forSubject) {

		final Optional<RDFNode> optionalResource = Optional.ofNullable(resource);
		final Optional<NodeType> optionalResourceNodeType = NodeTypeUtils.getNodeType(optionalResource);

		final Optional<String> optionalResourceId;
		final Optional<String> optionalResourceUri;
		final Optional<String> optionalDataModelUri;
		final Optional<String> optionalResourceValue;

		if (optionalResource.isPresent()) {

			if (optionalResourceNodeType.isPresent()) {

				if (NodeType.BNode.equals(optionalResourceNodeType.get())) {

					// only bnodes have ids in Jena

					optionalResourceId = Optional.ofNullable(((Resource) resource).getId().toString());
				} else {

					optionalResourceId = Optional.empty();
				}

				if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

					final Resource resourceResourceNode = (Resource) resource;

					optionalResourceUri = Optional.ofNullable(resourceResourceNode.getURI());
					optionalDataModelUri = Optional.empty();
					optionalResourceValue = Optional.empty();
				} else if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

					optionalResourceValue = Optional.ofNullable(((Literal) resource).getValue().toString());
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
				optionalResourceId = Optional.empty();
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
}
