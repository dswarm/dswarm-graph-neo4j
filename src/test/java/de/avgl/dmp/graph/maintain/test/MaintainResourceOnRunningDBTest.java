package de.avgl.dmp.graph.maintain.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public class MaintainResourceOnRunningDBTest extends RunningNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(MaintainResourceOnRunningDBTest.class);

	public MaintainResourceOnRunningDBTest() {

		super("/ext");
	}

	@Test
	public void testPing() throws IOException {

		LOG.debug("start ping test for Maintain resource at running DB");

		final ClientResponse response = service().path("/maintain/ping").get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for Maintain resource at running DB");
	}
}
