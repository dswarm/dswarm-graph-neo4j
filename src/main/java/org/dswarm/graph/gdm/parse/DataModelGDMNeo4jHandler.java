package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.parse.DataModelNeo4jHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelGDMNeo4jHandler extends GDMNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelGDMNeo4jHandler.class);

	public DataModelGDMNeo4jHandler(final GDMNeo4jProcessor processorArg) throws DMPGraphException {

		super(new DataModelNeo4jHandler(processorArg.getProcessor()), processorArg);
	}
}
