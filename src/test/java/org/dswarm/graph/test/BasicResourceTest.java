package org.dswarm.graph.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;

/**
 * @author tgaengler
 */
public abstract class BasicResourceTest extends Neo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(BasicResourceTest.class);

	public BasicResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String resourceArg, final String dbTypeArg) {

		super(neo4jDBWrapper, resourceArg, dbTypeArg);
	}

	@Test
	public void testPingToDB() throws IOException {

		LOG.debug("start ping test for GDM resource at " + dbType + " DB");

		final ClientResponse response = target().path("/ping").get(ClientResponse.class);

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for GDM resource at " + dbType + " DB");
	}
}
