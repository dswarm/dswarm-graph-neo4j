package de.avgl.dmp.graph.test;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * @author tgaengler
 */
public abstract class EmbeddedNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(EmbeddedNeo4jTest.class);

	protected final String		MOUNT_POINT;
	protected NeoServer			server;

	protected final int			serverPort;

	public EmbeddedNeo4jTest(final String mountEndpoint) {

		final URL resource = Resources.getResource("dmpgraph.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dmp.properties", e);
		}

		serverPort = Integer.valueOf(properties.getProperty("embedded_neo4j_server_port", "7499")).intValue();

		MOUNT_POINT = mountEndpoint;
	}

	@Before
	public void prepare() throws IOException {

		server = CommunityServerBuilder.server().onPort(serverPort).withThirdPartyJaxRsPackage("de.avgl.dmp.graph.resources", MOUNT_POINT).build();

		server.start();
	}

	protected WebResource service() {

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri().resolve(MOUNT_POINT));

		return service;
	}

	@After
	public void tearDown() {

		if (server != null) {

			server.stop();
		}
	}
}
