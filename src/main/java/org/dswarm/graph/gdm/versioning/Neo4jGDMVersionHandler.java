package org.dswarm.graph.gdm.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.BaseNeo4jGDMProcessor;

/**
 * @author tgaengler
 */
public class Neo4jGDMVersionHandler extends BaseNeo4jGDMVersionHandler {

	public Neo4jGDMVersionHandler(final BaseNeo4jGDMProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override protected int retrieveLatestVersion() {

		// TODO:

		return 0;
	}

	@Override public void updateLatestVersion() throws DMPGraphException {

		// TODO:
	}
}
