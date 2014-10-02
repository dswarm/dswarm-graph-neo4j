package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.parse.Handler;
import org.dswarm.graph.parse.Neo4jHandler;

/**
 * @author tgaengler
 */
public interface GDMHandler extends Handler {

	public void handleStatement(final Statement st, final Resource resource, final long index) throws DMPGraphException;

}
