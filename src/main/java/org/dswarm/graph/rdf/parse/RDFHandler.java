package org.dswarm.graph.rdf.parse;

import com.hp.hpl.jena.rdf.model.Statement;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public interface RDFHandler {

	public void handleStatement(Statement st) throws DMPGraphException;

	public void closeTransaction();
}
