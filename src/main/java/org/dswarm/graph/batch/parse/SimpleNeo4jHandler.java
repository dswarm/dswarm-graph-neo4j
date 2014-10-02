package org.dswarm.graph.batch.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.batch.Neo4jProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 * 
 * @author tgaengler
 */
public class SimpleNeo4jHandler extends BaseNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleNeo4jHandler.class);

	public SimpleNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}
}
