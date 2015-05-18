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
package org.dswarm.graph.maintain.test;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class MaintainResourceTest extends BasicResourceTest {

	private static final Logger LOG = LoggerFactory.getLogger(MaintainResourceTest.class);

	public MaintainResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/maintain", dbTypeArg);
	}

	/**
	 * note: the first schema indices initialisation is done via @Before
	 *
	 * @throws Exception
	 */
	@Test
	public void testInitSchemaIndicesMultipleTimes() throws Exception {

		MaintainResourceTest.LOG.debug("start init schema indices test for maintain resource at {} DB", dbType);

		final ClientResponse response = target().path("/schemaindices").post(ClientResponse.class, "");

		System.out.println("response = " + response);

		Assert.assertEquals("expected 200", 200, response.getStatus());

	}
}