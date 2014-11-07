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
package org.dswarm.graph.rdf.nx;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.StatementBuilder;
import org.dswarm.graph.rdf.nx.utils.NodeTypeUtils;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public abstract class RDFNeo4jProcessor {

	private static final Logger		LOG	= LoggerFactory.getLogger(RDFNeo4jProcessor.class);

	protected final Neo4jProcessor processor;

	public RDFNeo4jProcessor(final Neo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public Neo4jProcessor getProcessor() {

		return processor;
	}

	public StatementBuilder determineNode(final Node resource, final StatementBuilder statementBuilder, final boolean forSubject) {

		final Optional<Node> optionalResource = Optional.fromNullable(resource);
		final Optional<NodeType> optionalResourceNodeType = NodeTypeUtils.getNodeType(optionalResource);

		final Optional<String> optionalResourceId;
		final Optional<String> optionalResourceUri;
		final Optional<String> optionalDataModelUri;
		final Optional<String> optionalResourceValue;

		if (optionalResource.isPresent()) {

			if (optionalResourceNodeType.isPresent()) {

				if (NodeType.BNode.equals(optionalResourceNodeType.get())) {

					// only bnodes have ids in Jena

					optionalResourceId = Optional.fromNullable(resource.getLabel());
				} else {

					optionalResourceId = Optional.absent();
				}

				if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {

					optionalResourceUri = Optional.fromNullable(resource.getLabel());
					optionalDataModelUri = Optional.absent();
					optionalResourceValue = Optional.absent();
				} else if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

					// TODO: we could extract and set the datatype of the literal as well
					optionalResourceValue = Optional.fromNullable(resource.getLabel());
					optionalResourceUri = Optional.absent();
					optionalDataModelUri = Optional.absent();
				} else {

					optionalResourceUri = Optional.absent();
					optionalDataModelUri = Optional.absent();
					optionalResourceValue = Optional.absent();
				}
			} else {

				optionalResourceUri = Optional.absent();
				optionalDataModelUri = Optional.absent();
				optionalResourceValue = Optional.absent();
				optionalResourceId = Optional.absent();
			}
		} else {

			optionalResourceId = Optional.absent();
			optionalResourceUri = Optional.absent();
			optionalDataModelUri = Optional.absent();
			optionalResourceValue = Optional.absent();
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
