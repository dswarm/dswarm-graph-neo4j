package org.dswarm.graph.batch.rdf.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.parse.BaseNeo4jHandler;
import org.dswarm.graph.batch.rdf.RDFNeo4jProcessor;
import org.dswarm.graph.model.StatementBuilder;
import org.dswarm.graph.parse.Neo4jHandler;
import org.dswarm.graph.rdf.parse.RDFHandler;
import org.dswarm.graph.rdf.utils.NodeTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Statement;

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

		final RDFNode subject = st.getSubject();
		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeType(Optional.of(subject));
		sb.setOptionalSubjectNodeType(optionalSubjectNodeType);
		processor.determineNode(subject, sb, true);

		final Property predicate = st.getPredicate();
		final String predicateName = predicate.getURI();
		sb.setOptionalPredicateURI(Optional.fromNullable(predicateName));

		final RDFNode object = st.getObject();
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeType(Optional.of(object));
		sb.setOptionalObjectNodeType(optionalObjectNodeType);
		processor.determineNode(object, sb, false);

		final org.dswarm.graph.model.Statement statement = sb.build();

		handler.handleStatement(statement);
	}
}
