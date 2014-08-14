package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public interface GDMHandler {

	public void handleStatement(Statement st, Resource resource, long index);

	public void setResourceUri(String resourceUri);

	public void closeTransaction();
}
