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
package org.dswarm.graph.test;

import java.io.IOException;

import org.junit.Assert;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * @author tgaengler
 */
public abstract class Neo4jTest {

	private static final Logger		LOG	= LoggerFactory.getLogger(Neo4jTest.class);

	protected final Neo4jDBWrapper	neo4jDBWrapper;

	protected final String			resource;

	protected final String		dbType;

	public Neo4jTest(final Neo4jDBWrapper neo4jDBWrapperArg, final String resourceArg, final String dbTypeArg) {

		neo4jDBWrapper = neo4jDBWrapperArg;
		resource = resourceArg;
		dbType = dbTypeArg;
	}

	@Before
	public void prepare() throws IOException {

		neo4jDBWrapper.startServer();
		initIndices();
	}

	protected WebResource service() {

		return neo4jDBWrapper.service();
	}

	protected WebResource base() {

		return neo4jDBWrapper.base();
	}

	protected WebResource target() {

		return service().path(resource);
	}

	protected WebResource cypher() {

		return base().path("/db/data/cypher");
	}

	@After
	public void tearDown() {

		if (neo4jDBWrapper.checkServer()) {

			// TODO: we may need to remove this and replace this with a more precise delete method

			LOG.debug("clean-up DB after test has finished");

			final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

			Assert.assertEquals("expected 200", 200, response.getStatus());

			neo4jDBWrapper.stopServer();
		}
	}

	private void initIndices() {

		if (neo4jDBWrapper.checkServer()) {

			LOG.debug("init indices before everything can be tested");

			final ClientResponse response = service().path("/maintain/schemaindices").post(ClientResponse.class, "");

			Assert.assertEquals("expected 200", 200, response.getStatus());
		}
	}
}
