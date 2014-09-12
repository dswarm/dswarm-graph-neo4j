package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;

/**
 *
 * @author tgaengler
 *
 */
public interface GDMParser {

	/**
	 * Sets the GDMHandler that will handle the parsed GDM data.
	 */
	public void setGDMHandler(GDMHandler handler);

	public void parse() throws DMPGraphException;
}
