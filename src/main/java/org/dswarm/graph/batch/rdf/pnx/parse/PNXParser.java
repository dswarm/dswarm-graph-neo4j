package org.dswarm.graph.batch.rdf.pnx.parse;

import java.util.Iterator;

import de.knutwalker.ntparser.Statement;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public class PNXParser implements RDFParser {

	private final RDFHandler rdfHandler;

	public PNXParser(final RDFHandler handlerArg) {

		rdfHandler = handlerArg;
	}

	@Override
	public void parse(final Iterator<Statement> model) throws DMPGraphException {


		while (model.hasNext()) {

			rdfHandler.handleStatement(model.next());
		}
	}
}
