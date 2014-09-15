package org.dswarm.graph.rdf.parse.nx;

import org.dswarm.graph.DMPGraphException;

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

	public void parse() throws DMPGraphException;
}
