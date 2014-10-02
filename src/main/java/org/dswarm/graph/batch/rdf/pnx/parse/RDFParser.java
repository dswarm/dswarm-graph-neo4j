package org.dswarm.graph.batch.rdf.pnx.parse;

import java.util.Iterator;

import org.dswarm.graph.DMPGraphException;

import de.knutwalker.ntparser.Statement;

/**
 * @author tgaengler
 */
public interface RDFParser {

	public void parse(final Iterator<Statement> model) throws DMPGraphException;
}
