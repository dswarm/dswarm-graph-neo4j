package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class GDMResourceParser implements GDMParser {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResourceParser.class);

	private GDMHandler			gdmHandler;
	private final Resource		resource;

	public GDMResourceParser(final Resource resourceArg) {

		resource = resourceArg;
	}

	@Override
	public void setGDMHandler(final GDMHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public void parse() throws DMPGraphException {

		if (resource == null || resource.getStatements() == null || resource.getStatements().isEmpty()) {

			LOG.debug("there are no statements in the GDM resource");

			gdmHandler.getHandler().closeTransaction();

			return;
		}

		long i = 0;

		for (final Statement statement : resource.getStatements()) {

			i++;

			// note: just increasing the counter probably won't work at an update ;)

			gdmHandler.handleStatement(statement, resource, i);
		}

		gdmHandler.getHandler().closeTransaction();
	}
}
