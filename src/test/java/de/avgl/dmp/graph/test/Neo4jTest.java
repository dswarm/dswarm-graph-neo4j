package de.avgl.dmp.graph.test;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * @author tgaengler
 */
public abstract class Neo4jTest {

	private static final Logger		LOG	= LoggerFactory.getLogger(Neo4jTest.class);

	protected final Neo4jDBWrapper	neo4jDBWrapper;

	protected final String			resource;
	
	protected final String		dbType;

	public Neo4jTest(final Neo4jDBWrapper neo4jDBWrapperArg, final String resourceArg, final String dbTypeArg) {

		neo4jDBWrapper = neo4jDBWrapperArg;
		resource = resourceArg;
		dbType = dbTypeArg;
	}

	@Before
	public void prepare() throws IOException {

		neo4jDBWrapper.startServer();
	}

	protected WebResource service() {

		return neo4jDBWrapper.service();
	}

	protected WebResource target() {

		return service().path(resource);
	}

	@After
	public void tearDown() {

		if (neo4jDBWrapper.checkServer()) {

			// TODO: we may need to remove this and replace this with a more precise delete method

			LOG.debug("clean-up DB after test has finished");

			final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

			Assert.assertEquals("expected 200", 200, response.getStatus());

			neo4jDBWrapper.stopServer();
		}
	}
}
