package org.dswarm.graph.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;

/**
 * @author tgaengler
 */
public class SimpleNeo4jVersionHandler extends Neo4jVersionHandler {

	public SimpleNeo4jVersionHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override
	protected int retrieveLatestVersion() {

		// TODO:

		return 0;
	}

	@Override
	public void updateLatestVersion() throws DMPGraphException {

		// TODO:
	}
}
