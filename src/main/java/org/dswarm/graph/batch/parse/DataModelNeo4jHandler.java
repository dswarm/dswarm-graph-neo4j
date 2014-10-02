package org.dswarm.graph.batch.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.batch.Neo4jProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelNeo4jHandler extends BaseNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelNeo4jHandler.class);

	public DataModelNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}
}
