package org.dswarm.graph.gdm.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;

/**
 * @author tgaengler
 */
public class SimpleGDMNeo4jVersionHandler extends GDMNeo4jVersionHandler {

	public SimpleGDMNeo4jVersionHandler(final GDMNeo4jProcessor processorArg) throws DMPGraphException {

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
