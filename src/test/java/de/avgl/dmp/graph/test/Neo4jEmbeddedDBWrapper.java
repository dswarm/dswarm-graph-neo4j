package de.avgl.dmp.graph.test;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

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
public class Neo4jEmbeddedDBWrapper implements Neo4jDBWrapper {

	private static final Logger	LOG	= LoggerFactory.getLogger(Neo4jEmbeddedDBWrapper.class);

	private final String		MOUNT_POINT;
	private NeoServer			server;

	private final int			serverPort;

	public Neo4jEmbeddedDBWrapper(final String mountEndpoint) {

		final URL resource = Resources.getResource("dmpgraph.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dmpgraph.properties", e);
		}

		serverPort = Integer.valueOf(properties.getProperty("embedded_neo4j_server_port", "7499")).intValue();

		MOUNT_POINT = mountEndpoint;
	}

	public void startServer() throws IOException {

		server = CommunityServerBuilder.server().onPort(serverPort).withThirdPartyJaxRsPackage("de.avgl.dmp.graph.resources", MOUNT_POINT).build();

		server.start();
	}

	public WebResource service() {

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri().resolve(MOUNT_POINT));

		return service;
	}

	@Override public WebResource base() {

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri());

		return service;
	}

	@Override
	public boolean checkServer() {

		return server != null;
	}

	@Override
	public void stopServer() {

		server.stop();
	}
}
