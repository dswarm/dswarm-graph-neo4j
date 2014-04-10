package de.avgl.dmp.graph.rdf.parse.nx;

import org.semanticweb.yars.nx.Node;

/**
 * @author tgaengler
 */
public interface RDFHandler {

	public void handleStatement(Node[] st);

	public void closeTransaction();
}
