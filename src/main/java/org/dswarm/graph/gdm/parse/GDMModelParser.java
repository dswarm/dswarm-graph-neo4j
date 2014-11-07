/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.gdm.parse;

import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
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
	public void parse() throws DMPGraphException {

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

			gdmHandler.getHandler().setResourceUri(resource.getUri());

			for (final Statement statement : statements) {

				i++;

				// note: just increasing the counter probably won't work at an update ;)

				gdmHandler.handleStatement(statement, resource, i);
			}
		}
	}
}
