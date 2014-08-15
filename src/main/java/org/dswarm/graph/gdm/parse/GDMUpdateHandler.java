package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public interface GDMUpdateHandler {

	public void handleStatement(Statement st, Resource resource, long index);

	public void deprecateStatement(long index);

	public Node deprecateStatement(String uuid) throws DMPGraphException;

	public int getLatestVersion();

	public void setResourceUri(String resourceUri);

	public void closeTransaction();
}
