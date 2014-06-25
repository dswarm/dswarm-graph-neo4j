package org.dswarm.graph.gdm.parse;

import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public class GDMModelParser implements GDMParser {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMModelParser.class);

	private GDMHandler			gdmHandler;
	private final Model			model;

	public GDMModelParser(final Model modelArg) {

		model = modelArg;
	}

	@Override
	public void setGDMHandler(final GDMHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public void parse() {

		final Collection<Resource> resources = model.getResources();

		if (resources == null || resources.isEmpty()) {

			LOG.debug("there are no resources in the GDM model");

			return;
		}

		for (final Resource resource : resources) {

			Set<Statement> statements = resource.getStatements();

			if (statements == null || statements.isEmpty()) {

				LOG.debug("there are no statements for resource '" + resource.getUri() + "' in the GDM model");

				continue;
			}

			long i = 0;

			for (final Statement statement : statements) {

				i++;

				// note: just increasing the counter probably won't work at an update ;)

				gdmHandler.handleStatement(statement, resource, i);
			}
		}

		gdmHandler.closeTransaction();
	}
}
