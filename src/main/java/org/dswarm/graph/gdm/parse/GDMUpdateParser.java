package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;

/**
 *
 * @author tgaengler
 *
 */
public interface GDMUpdateParser {

	/**
	 * Sets the GDMUpdateHandler that will handle the parsed GDM data.
	 */
	public void setGDMHandler(GDMUpdateHandler handler);

	public void parse() throws DMPGraphException;
}
