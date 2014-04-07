package de.avgl.dmp.graph.rdf.nx.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.EmbeddedNeo4jTest;

public class RDFResourceOnEmbeddedDBNxTest extends EmbeddedNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFResourceOnEmbeddedDBNxTest.class);

	public RDFResourceOnEmbeddedDBNxTest() {

		super("/ext");
	}

	@Test
	public void testPingToTestDB() throws IOException {

		LOG.debug("start ping test for RDF resource at embedded DB");

		final ClientResponse response = service().path("/rdf/ping").get(ClientResponse.class);

		String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for RDF resource at embedded DB");
	}

	@Test
	public void writeRDFToTestDB() throws IOException {

		LOG.debug("start write test for RDF resource at embedded DB");

		writeRDFToTestDBInternal(server);

		LOG.debug("finished write test for RDF resource at embedded DB");
	}

	private void writeRDFToTestDBInternal(final NeoServer server) throws IOException {

		LOG.debug("start writing RDF statements for RDF resource at embedded DB");

		final URL fileURL = Resources.getResource("dmpf_bsp1.nt");
		final byte[] file = Resources.toByteArray(fileURL);

		// POST the request
		final ClientResponse response = service().path("/rdf/putnx").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, file);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing RDF statements for RDF resource at embedded DB");
	}
}
