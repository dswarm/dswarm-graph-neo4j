package org.dswarm.graph.rdf.parse;

import com.hp.hpl.jena.rdf.model.Statement;

/**
 * @author tgaengler
 */
public interface RDFHandler {

	public void handleStatement(Statement st);

	public void closeTransaction();
}
