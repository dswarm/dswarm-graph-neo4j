package org.dswarm.graph.batch.rdf.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.batch.parse.DataModelNeo4jHandler;
import org.dswarm.graph.batch.rdf.RDFNeo4jProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelRDFNeo4jHandler extends RDFNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelRDFNeo4jHandler.class);

	public DataModelRDFNeo4jHandler(final RDFNeo4jProcessor processorArg) throws DMPGraphException {

		super(new DataModelNeo4jHandler(processorArg.getProcessor()), processorArg);
	}
}
