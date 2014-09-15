package org.dswarm.graph.rdf.parse.nx;

import org.semanticweb.yars.nx.parser.NxParser;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public class NxModelParser implements RDFParser {

	private RDFHandler		rdfHandler;
	private final NxParser	model;

	public NxModelParser(final NxParser modelArg) {

		model = modelArg;
	}

	@Override
	public void setRDFHandler(final RDFHandler handler) {

		rdfHandler = handler;
	}

	@Override
	public void parse() throws DMPGraphException {

		while (model.hasNext()) {

			rdfHandler.handleStatement(model.next());
		}

		rdfHandler.closeTransaction();
	}
}
