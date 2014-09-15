package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public interface GDMHandler {

	public void handleStatement(final Statement st, final Resource resource, final long index) throws DMPGraphException;

	public void setResourceUri(String resourceUri);

	public void closeTransaction();

}
