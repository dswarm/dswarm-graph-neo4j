package org.dswarm.graph.rdf.parse;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * @author tgaengler
 */
public class JenaModelParser implements RDFParser {

	private RDFHandler	rdfHandler;
	private final Model	model;

	public JenaModelParser(final Model modelArg) {

		model = modelArg;
	}

	@Override
	public void setRDFHandler(final RDFHandler handler) {

		rdfHandler = handler;
	}

	@Override
	public void parse() {

		final StmtIterator iter = model.listStatements();

		while (iter.hasNext()) {

			rdfHandler.handleStatement(iter.next());
		}

		rdfHandler.closeTransaction();
	}
}
