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
package org.dswarm.graph.batch.rdf.pnx.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.batch.parse.BaseNeo4jHandler;
import org.dswarm.graph.batch.rdf.pnx.RDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.utils.NodeTypeUtils;
import org.dswarm.graph.model.StatementBuilder;
import org.dswarm.graph.parse.Neo4jHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import de.knutwalker.ntparser.Node;
import de.knutwalker.ntparser.Resource;
import de.knutwalker.ntparser.Statement;

/**
 * @author tgaengler
 */
public abstract class RDFNeo4jHandler implements RDFHandler {

	private static final Logger			LOG	= LoggerFactory.getLogger(RDFNeo4jHandler.class);

	protected final BaseNeo4jHandler	handler;
	protected final RDFNeo4jProcessor	processor;

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
