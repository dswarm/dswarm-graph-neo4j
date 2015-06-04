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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.utils.NamespaceUtils;

/**
 * @author tgaengler
 */
public class GDMResourceParser implements GDMParser {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResourceParser.class);

	private GDMHandler			gdmHandler;
	private final Resource		resource;
	private long parsedResources = 0;

	public GDMResourceParser(final Resource resourceArg) {

		resource = resourceArg;
	}

	@Override
	public void setGDMHandler(final GDMHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public Observable<Void> parse() throws DMPGraphException {

		parsedResources = 0;

		if (resource == null || resource.getStatements() == null || resource.getStatements().isEmpty()) {

			LOG.debug("there are no statements in the GDM resource");

			((Neo4jDeltaGDMHandler) gdmHandler).closeTransaction();

			return Observable.empty();
		}

		long i = 0;

		final String prefixedResourceUri = gdmHandler.getNamespaceIndex().createPrefixedURI(resource.getUri());
		final long resourceHash = HashUtils.generateHash(prefixedResourceUri);

		for (final Statement statement : resource.getStatements()) {

			i++;

			// note: just increasing the counter probably won't work at an update ;)

			gdmHandler.handleStatement(statement, resourceHash, i);
		}

		((Neo4jDeltaGDMHandler) gdmHandler).closeTransaction();

		parsedResources++;

		// TODO: is that correct here?
		return Observable.empty();
	}

	@Override public long parsedResources() {

		return parsedResources;
	}
}
