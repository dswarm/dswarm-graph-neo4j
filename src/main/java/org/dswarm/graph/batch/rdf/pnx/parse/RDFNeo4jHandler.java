package org.dswarm.graph.batch.rdf.pnx.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.parse.BaseNeo4jHandler;
import org.dswarm.graph.batch.rdf.pnx.RDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.utils.NodeTypeUtils;
import org.dswarm.graph.model.StatementBuilder;
import org.dswarm.graph.parse.Neo4jHandler;

import de.knutwalker.ntparser.Node;
import de.knutwalker.ntparser.Resource;
import de.knutwalker.ntparser.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public abstract class RDFNeo4jHandler implements RDFHandler {

	private static final Logger			LOG	= LoggerFactory.getLogger(RDFNeo4jHandler.class);

	protected final BaseNeo4jHandler  handler;
	protected final RDFNeo4jProcessor processor;

	public RDFNeo4jHandler(final BaseNeo4jHandler handlerArg, final RDFNeo4jProcessor processorArg) throws DMPGraphException {

		handler = handlerArg;
		processor = processorArg;
	}

	@Override
	public Neo4jHandler getHandler() {

		return handler;
	}

	@Override
	public void handleStatement(final Statement st) throws DMPGraphException {

		final StatementBuilder sb = new StatementBuilder();

		final Node subject = st.s();
		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeType(Optional.of(subject));
		sb.setOptionalSubjectNodeType(optionalSubjectNodeType);
		processor.determineNode(subject, sb, true);

		final Resource predicate = st.p();
		final String predicateName = predicate.toString();
		sb.setOptionalPredicateURI(Optional.fromNullable(predicateName));

		final Node object = st.o();
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeType(Optional.of(object));
		sb.setOptionalObjectNodeType(optionalObjectNodeType);
		processor.determineNode(object, sb, false);

		final org.dswarm.graph.model.Statement statement = sb.build();

		handler.handleStatement(statement);
	}
}
