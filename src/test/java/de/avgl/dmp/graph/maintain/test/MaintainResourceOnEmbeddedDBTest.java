package de.avgl.dmp.graph.maintain.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.EmbeddedNeo4jTest;

public class MaintainResourceOnEmbeddedDBTest extends EmbeddedNeo4jTest {

	public MaintainResourceOnEmbeddedDBTest() {

		super("/ext");
	}

	@Test
	public void testPingToTestDB() throws IOException {

		final ClientResponse response = service().path("/maintain/ping").get(ClientResponse.class);

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);
	}
}
