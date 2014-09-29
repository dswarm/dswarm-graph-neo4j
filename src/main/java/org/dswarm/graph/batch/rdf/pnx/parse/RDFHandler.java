package org.dswarm.graph.batch.rdf.pnx.parse;

import de.knutwalker.ntparser.Statement;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.parse.Handler;

/**
 * @author tgaengler
 */
public interface RDFHandler extends Handler {

	public void handleStatement(final Statement st) throws DMPGraphException;
}
