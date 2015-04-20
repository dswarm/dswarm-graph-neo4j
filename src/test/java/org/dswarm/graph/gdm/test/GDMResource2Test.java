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
package org.dswarm.graph.gdm.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import org.junit.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;

import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public abstract class GDMResource2Test extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResource2Test.class);

	public GDMResource2Test(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);
	}

	@Test
	public void writeGDMToTestDB() throws IOException {

		LOG.debug("start write test for GDM resource at {} DB", dbType);

		writeGDMToTestDBInternal();

		LOG.debug("finished write test for GDM resource at {} DB", dbType);
	}

	private void writeGDMToTestDBInternal() throws IOException {

		LOG.debug("start writing GDM statements for RDF resource at {} DB", dbType);

		final URL fileURL = Resources.getResource("test-mabxml.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		// POST the request
		final ClientResponse response = target().path("/put").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, file);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing GDM statements for RDF resource at {} DB", dbType);
	}
}
