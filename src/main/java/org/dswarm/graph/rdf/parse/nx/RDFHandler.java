package org.dswarm.graph.rdf.parse.nx;

import org.semanticweb.yars.nx.Node;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public interface RDFHandler {

	public void handleStatement(Node[] st) throws DMPGraphException;

	public void closeTransaction();
}
