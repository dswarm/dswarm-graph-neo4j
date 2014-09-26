package org.dswarm.graph.batch.rdf.pnx;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.Neo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.utils.NodeTypeUtils;
import org.dswarm.graph.model.StatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.knutwalker.dbpedia.Node;

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

					optionalResourceId = Optional.fromNullable(resource.toString());
				} else {

					optionalResourceId = Optional.absent();
				}

				if (NodeType.Resource.equals(optionalResourceNodeType.get()) || NodeType.TypeResource.equals(optionalResourceNodeType.get())) {


					optionalResourceUri = Optional.fromNullable(resource.toString());
					optionalDataModelUri = Optional.absent();
					optionalResourceValue = Optional.absent();
				} else if (NodeType.Literal.equals(optionalResourceNodeType.get())) {

					optionalResourceValue = Optional.fromNullable(resource.toString());
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
