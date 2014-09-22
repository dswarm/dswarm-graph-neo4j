package org.dswarm.graph.rdf.parse;

import com.hp.hpl.jena.rdf.model.Statement;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.parse.Handler;
import org.dswarm.graph.parse.Neo4jHandler;

/**
 * @author tgaengler
 */
public interface RDFHandler extends Handler {

	public void handleStatement(final Statement st) throws DMPGraphException;
}
