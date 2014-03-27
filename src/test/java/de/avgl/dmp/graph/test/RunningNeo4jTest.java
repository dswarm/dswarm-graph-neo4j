package de.avgl.dmp.graph.test;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.Client;
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

			LOG.error("Could not load dmp.properties", e);
		}

		graphEndpoint = properties.getProperty("dmp_graph_endpoint", "http://localhost:7474/graph");

		MOUNT_POINT = mountEndpoint;
	}

	protected WebResource service() {

		final Client c = Client.create();
		final WebResource service = c.resource(graphEndpoint);

		return service;
	}
}
