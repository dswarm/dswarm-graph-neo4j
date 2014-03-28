package de.avgl.dmp.graph.maintain.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.EmbeddedNeo4jTest;

public class MaintainResourceOnEmbeddedDBTest extends EmbeddedNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(MaintainResourceOnEmbeddedDBTest.class);

	public MaintainResourceOnEmbeddedDBTest() {

		super("/ext");
	}

	@Test
	public void testPingToTestDB() throws IOException {

		LOG.debug("start ping test for Maintain resource at embedded DB");

		final ClientResponse response = service().path("/maintain/ping").get(ClientResponse.class);

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for Maintain resource at embedded DB");
	}
}
