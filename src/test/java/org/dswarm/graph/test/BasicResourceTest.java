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

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;

/**
 * @author tgaengler
 */
public abstract class BasicResourceTest extends Neo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(BasicResourceTest.class);

	public BasicResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String resourceArg, final String dbTypeArg) {

		super(neo4jDBWrapper, resourceArg, dbTypeArg);
	}

	@Test
	public void testPingToDB() throws IOException {

		LOG.debug("start ping test for GDM resource at " + dbType + " DB");

		final ClientResponse response = target().path("/ping").get(ClientResponse.class);

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for GDM resource at " + dbType + " DB");
	}
}
