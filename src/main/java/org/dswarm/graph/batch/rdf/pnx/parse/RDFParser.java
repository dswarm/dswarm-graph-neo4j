package org.dswarm.graph.batch.rdf.pnx.parse;

import java.util.Iterator;

import de.knutwalker.ntparser.Statement;

import org.dswarm.graph.DMPGraphException;

/**
 *
 * @author tgaengler
 *
 */
public interface RDFParser {

	public void parse(final Iterator<Statement> model) throws DMPGraphException;
}
