package org.dswarm.graph.batch.rdf.pnx.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.parse.Handler;

import de.knutwalker.ntparser.Statement;

/**
 * @author tgaengler
 */
public interface RDFHandler extends Handler {

	public void handleStatement(final Statement st) throws DMPGraphException;
}
