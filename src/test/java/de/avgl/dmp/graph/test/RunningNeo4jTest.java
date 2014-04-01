package de.avgl.dmp.graph.test;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class RunningNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(RunningNeo4jTest.class);

	protected final String		MOUNT_POINT;

	protected final String		graphEndpoint;

	public RunningNeo4jTest(final String mountEndpoint) {

		final URL resource = Resources.getResource("dmpgraph.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dmpgraph.properties", e);
		}

		graphEndpoint = properties.getProperty("dmp_graph_endpoint", "http://localhost:7474/graph");

		MOUNT_POINT = mountEndpoint;
	}

	
	@After
	public void tearDown() {

		// TODO: we may need to remove this and replace this with a more precise delete method
		
		LOG.debug("clean-up DB after test has finished");

		final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
	}

	protected WebResource service() {

		final Client c = Client.create();
		final WebResource service = c.resource(graphEndpoint);

		return service;
	}
}
