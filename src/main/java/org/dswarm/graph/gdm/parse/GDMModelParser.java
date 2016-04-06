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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public class GDMModelParser implements GDMParser {

	private static final Logger LOG = LoggerFactory.getLogger(GDMModelParser.class);

	private GDMHandler gdmHandler;
	private final Observable<Resource> model;
	private long parsedResources = 0;

	public GDMModelParser(final Observable<Resource> modelArg) {

		model = modelArg;
	}

	@Override
	public void setGDMHandler(final GDMHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public Observable<Boolean> parse() throws DMPGraphException {

		parsedResources = 0;

		if (model == null) {

			LOG.debug("there are no resources in the GDM model");

			return Observable.empty();
		}

		return model.map(resource -> {

			final Set<Statement> statements = resource.getStatements();

			if (statements == null || statements.isEmpty()) {

				LOG.debug("there are no statements for resource '{}' in the GDM model", resource.getUri());

				return Boolean.FALSE;
			}

			AtomicLong counter = new AtomicLong(0);

			try {

				final String prefixedResourceUri = gdmHandler.getHandler().getProcessor().createPrefixedURI(resource.getUri());
				final long resourceHash = gdmHandler.getHandler().getProcessor().generateResourceHash(prefixedResourceUri, Optional.empty());

				gdmHandler.getHandler().setResourceUri(prefixedResourceUri);
				gdmHandler.getHandler().setResourceHash(resourceHash);
				gdmHandler.getHandler().resetResourceIndexCounter();

				for (final Statement statement : statements) {

					final long i = counter.incrementAndGet();

					// note: just increasing the counter probably won't work at an update ;)

					gdmHandler.handleStatement(statement, resourceHash, i);

				}
			} catch (final DMPGraphException e) {

				throw new RuntimeException(e);
			}

			parsedResources++;

			return Boolean.TRUE;
		});
	}

	@Override
	public long parsedResources() {

		return parsedResources;
	}
}
