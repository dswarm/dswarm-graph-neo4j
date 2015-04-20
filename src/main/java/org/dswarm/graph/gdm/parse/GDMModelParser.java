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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Func1;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public class GDMModelParser implements GDMParser {

	private static final Logger LOG = LoggerFactory.getLogger(GDMModelParser.class);

	private       GDMHandler           gdmHandler;
	private final Observable<Resource> model;

	public GDMModelParser(final Observable<Resource> modelArg) {

		model = modelArg;
	}

	@Override
	public void setGDMHandler(final GDMHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public void parse() throws DMPGraphException {

		if (model == null) {

			LOG.debug("there are no resources in the GDM model");

			return;
		}

		final Observable<Void> parsedStatements = model.map(new Func1<Resource, Void>() {

			@Override public Void call(Resource resource) {

				Set<Statement> statements = resource.getStatements();

				if (statements == null || statements.isEmpty()) {

					LOG.debug("there are no statements for resource '{}' in the GDM model", resource.getUri());

					return null;
				}

				long i = 0;

				gdmHandler.getHandler().setResourceUri(resource.getUri());

				for (final Statement statement : statements) {

					i++;

					// note: just increasing the counter probably won't work at an update ;)

					try {

						gdmHandler.handleStatement(statement, resource.getUri(), i);
					} catch (DMPGraphException e) {

						throw new RuntimeException(e);
					}
				}

				return null;
			}
		});

		try {

			parsedStatements.toBlocking().last();
		} catch (RuntimeException e) {
			throw new DMPGraphException(e.getMessage(), e.getCause());
		}
	}
}
