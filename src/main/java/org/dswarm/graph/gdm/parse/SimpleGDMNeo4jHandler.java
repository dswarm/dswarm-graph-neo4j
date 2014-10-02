package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.parse.SimpleNeo4jHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public class SimpleGDMNeo4jHandler extends GDMNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleGDMNeo4jHandler.class);

	public SimpleGDMNeo4jHandler(final GDMNeo4jProcessor processorArg) throws DMPGraphException {

		super(new SimpleNeo4jHandler(processorArg.getProcessor()), processorArg);
	}
}
