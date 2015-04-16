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
package org.dswarm.graph.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.versioning.SimpleNeo4jVersionHandler;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public class SimpleNeo4jHandler extends BaseNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleNeo4jHandler.class);

	public SimpleNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new SimpleNeo4jVersionHandler(processor);
	}
}
