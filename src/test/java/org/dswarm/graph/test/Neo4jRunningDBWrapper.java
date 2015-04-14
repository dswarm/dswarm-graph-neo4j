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

import org.junit.Assert;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 *
 * @author tgaengler
 *
 */
public class Neo4jRunningDBWrapper implements Neo4jDBWrapper {

	private static final Logger	LOG	= LoggerFactory.getLogger(Neo4jRunningDBWrapper.class);

	protected final String		graphEndpoint;

	public Neo4jRunningDBWrapper() {

		final URL resource = Resources.getResource("dmpgraph.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dmpgraph.properties", e);
		}

		graphEndpoint = properties.getProperty("dmp_graph_endpoint", "http://localhost:7474/graph");

		Neo4jRunningDBWrapper.LOG.info("DMP graph endpoint URI is = '{}'", graphEndpoint);
	}

	@After
	public void tearDown() {

		// TODO: we may need to remove this and replace this with a more precise delete method

		LOG.debug("clean-up DB after test has finished");

		final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
	}

	@Override
	public WebResource service() {

		final Client c = Client.create();

		return c.resource(graphEndpoint);
	}

	@Override public WebResource base() {

		final String baseURI = graphEndpoint.substring(0,graphEndpoint.lastIndexOf("/"));

		final Client c = Client.create();

		return c.resource(baseURI);
	}

	@Override
	public void startServer() throws IOException {

		// nothing to do here, the server should already run ;)

	}

	@Override
	public boolean checkServer() {

		// TODO add check for running server

		return true;
	}

	@Override
	public void stopServer() {

		// nothing to do here, the server should also run afterwards ;)
	}
}
