package de.avgl.dmp.graph.gdm.parse;



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
	
	public void parse();
}
