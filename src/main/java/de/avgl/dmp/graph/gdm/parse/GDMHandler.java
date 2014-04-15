package de.avgl.dmp.graph.gdm.parse;

import de.avgl.dmp.graph.json.Resource;
import de.avgl.dmp.graph.json.Statement;

/**
 * @author tgaengler
 */
public interface GDMHandler {

	public void handleStatement(Statement st, Resource resource, long index);
	
	public void closeTransaction();
}
