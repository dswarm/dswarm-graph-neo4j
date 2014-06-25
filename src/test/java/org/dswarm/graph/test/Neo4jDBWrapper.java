package org.dswarm.graph.test;

import java.io.IOException;

import com.sun.jersey.api.client.WebResource;

/**
 *
 * @author tgaengler
 *
 */
public interface Neo4jDBWrapper {

	WebResource service();

	WebResource base();

	void startServer() throws IOException;

	boolean checkServer();

	void stopServer();
}
