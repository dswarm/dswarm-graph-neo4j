package org.dswarm.graph.batch.rdf.pnx.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.batch.parse.SimpleNeo4jHandler;
import org.dswarm.graph.batch.rdf.pnx.RDFNeo4jProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 * 
 * @author tgaengler
 */
public class SimpleRDFNeo4jHandler extends RDFNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleRDFNeo4jHandler.class);

	public SimpleRDFNeo4jHandler(final RDFNeo4jProcessor processorArg) throws DMPGraphException {

		super(new SimpleNeo4jHandler(processorArg.getProcessor()), processorArg);
	}
}
