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
package org.dswarm.graph.rdf.pnx.test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public abstract class RDFResourceDBPNXTest extends BasicResourceTest {

	private static final Logger LOG = LoggerFactory.getLogger(RDFResourceDBPNXTest.class);

	public RDFResourceDBPNXTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/rdf", dbTypeArg);
	}

	@Test
	public void writeRDFToDB() throws IOException {

		LOG.debug("start write test for RDF resource at {} DB", dbType);

		writeRDFToDBInternal();

		LOG.debug("finished write test for RDF resource at {} DB", dbType);
	}

	private void writeRDFToDBInternal() throws IOException {

		LOG.debug("start writing RDF statements for RDF resource at {} DB", dbType);

		final URL fileURL = Resources.getResource("dmpf_bsp1.nt");
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream inputStream = byteSource.openStream();
		final InputStream in = new BufferedInputStream(inputStream, 1024);

		// POST the request
		final ClientResponse response = target().path("/putpnx").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, in);
		in.close();
		inputStream.close();

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing RDF statements for RDF resource at {} DB", dbType);
	}
}
