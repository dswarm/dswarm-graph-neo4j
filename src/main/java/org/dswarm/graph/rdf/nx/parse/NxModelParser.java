package org.dswarm.graph.rdf.nx.parse;

import org.dswarm.graph.DMPGraphException;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * @author tgaengler
 */
public class NxModelParser implements RDFParser {

	private       RDFHandler rdfHandler;
	private final NxParser   model;

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
	}
}
