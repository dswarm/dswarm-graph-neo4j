package org.dswarm.graph.rdf.nx.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.parse.Handler;
import org.semanticweb.yars.nx.Node;

/**
 * @author tgaengler
 */
public interface RDFHandler extends Handler {

	public void handleStatement(Node[] st) throws DMPGraphException;
}
