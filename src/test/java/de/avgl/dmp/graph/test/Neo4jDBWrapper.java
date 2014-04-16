package de.avgl.dmp.graph.test;

import java.io.IOException;

import com.sun.jersey.api.client.WebResource;

/**
 * 
 * @author tgaengler
 *
 */
public interface Neo4jDBWrapper {

	WebResource service();
	
	void startServer() throws IOException;
	
	boolean checkServer();
	
	void stopServer();
}
