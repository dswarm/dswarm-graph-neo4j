/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.test;

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

		server = CommunityServerBuilder.server().onPort(serverPort).withThirdPartyJaxRsPackage("org.dswarm.graph.resources", MOUNT_POINT).build();

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
