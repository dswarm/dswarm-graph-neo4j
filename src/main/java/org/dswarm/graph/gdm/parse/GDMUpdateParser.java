package org.dswarm.graph.gdm.parse;



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

	public void parse();
}
