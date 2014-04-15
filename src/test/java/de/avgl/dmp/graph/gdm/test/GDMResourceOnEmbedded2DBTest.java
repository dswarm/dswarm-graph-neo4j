package de.avgl.dmp.graph.gdm.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.EmbeddedNeo4jTest;

public class GDMResourceOnEmbedded2DBTest extends EmbeddedNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResourceOnEmbedded2DBTest.class);

	public GDMResourceOnEmbedded2DBTest() {

		super("/ext");
	}

	@Test
	public void testPingToTestDB() throws IOException {

		LOG.debug("start ping test for GDM resource at embedded DB");

		final ClientResponse response = service().path("/gdm/ping").get(ClientResponse.class);

		String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for GDM resource at embedded DB");
	}

	@Test
	public void writeGDMToTestDB() throws IOException {

		LOG.debug("start write test for GDM resource at embedded DB");

		writeGDMToTestDBInternal();

		LOG.debug("finished write test for GDM resource at embedded DB");
	}

	private void writeGDMToTestDBInternal() throws IOException {

		LOG.debug("start writing GDM statements for RDF resource at embedded DB");

		final URL fileURL = Resources.getResource("test-mabxml.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts

		// POST the request
		final ClientResponse response = service().path("/gdm/put").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, file);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing GDM statements for RDF resource at embedded DB");
	}
}
