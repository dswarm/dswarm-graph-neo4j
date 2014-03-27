package de.avgl.dmp.graph.maintain.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public class MaintainResourceOnRunningDBTest extends RunningNeo4jTest {

	public MaintainResourceOnRunningDBTest() {

		super("/ext");
	}

	@Test
	public void testPing() throws IOException {

		final ClientResponse response = service().path("/maintain/ping").get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected pong", "pong", body);
	}
}
