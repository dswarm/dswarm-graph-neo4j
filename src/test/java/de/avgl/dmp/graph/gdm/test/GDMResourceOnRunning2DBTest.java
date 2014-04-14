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

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public class GDMResourceOnRunning2DBTest extends RunningNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResourceOnRunning2DBTest.class);

	public GDMResourceOnRunning2DBTest() {

		super("/ext");
	}

	@Test
	public void writeGDMToRunningDB() throws IOException {

		LOG.debug("start write test for GDM resource at running DB");

		writeGDMToRunningDBInternal();

		LOG.debug("finished write test for GDM resource at running DB");
	}

	@Test
	public void testPing() throws IOException {

		LOG.debug("start ping test for GDM resource at running DB");

		final ClientResponse response = service().path("/gdm/ping").get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for GDM resource at running DB");
	}
	
	private void writeGDMToRunningDBInternal() throws IOException {
		
		LOG.debug("start writing GDM statements for GDM resource at embedded DB");

		final URL fileURL = Resources.getResource("test-mabxml.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts

		// POST the request
		final ClientResponse response = service().path("/gdm/put").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, file);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing GDM statements for GDM resource at embedded DB");
	}
}
