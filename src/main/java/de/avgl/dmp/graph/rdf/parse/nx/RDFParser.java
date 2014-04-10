package de.avgl.dmp.graph.rdf.parse.nx;



/**
 * 
 * @author tgaengler
 *
 */
public interface RDFParser {

	/**
	 * Sets the RDFHandler that will handle the parsed RDF data.
	 */
	public void setRDFHandler(RDFHandler handler);
	
	public void parse();
}
