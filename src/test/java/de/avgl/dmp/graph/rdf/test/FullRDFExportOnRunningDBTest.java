package de.avgl.dmp.graph.rdf.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public abstract class FullRDFExportOnRunningDBTest extends RunningNeo4jTest {

	protected static final String TEST_RDF_FILE = "dmpf_bsp1.n3";
	//protected static final String TEST_RDF_FILE = "turtle_untyped.ttl";
	//protected static final String TEST_RDF_FILE = "turtle_untyped_with_blanks.ttl";
	
	private static final Logger	LOG	= LoggerFactory.getLogger(FullRDFExportOnRunningDBTest.class);

	public FullRDFExportOnRunningDBTest(String mountEndpoint) {
		super(mountEndpoint);
	}

	@Test
	public void testPing() throws IOException {
	
		LOG.debug("start ping test for RDF resource at running DB");
	
		final ClientResponse response = service().path("/rdf/ping").get(ClientResponse.class);
	
		Assert.assertEquals("expected 200", 200, response.getStatus());
	
		final String body = response.getEntity(String.class);
	
		Assert.assertEquals("expected pong", "pong", body);
	
		LOG.debug("finished ping test for RDF resource at running DB");
	}

	protected void writeRDFToRunningDBInternal(String resource_graph_uri)
			throws IOException {
				
				LOG.debug("start writing RDF statements for RDF resource at running DB (to graph " +  resource_graph_uri + ")");
				
				final URL fileURL = Resources.getResource(TEST_RDF_FILE);
				final byte[] file = Resources.toByteArray(fileURL);
			
				// Construct a MultiPart with two body parts
				final MultiPart multiPart = new MultiPart();
				multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
						new BodyPart(resource_graph_uri, MediaType.TEXT_PLAIN_TYPE));
			
				// POST the request
				final ClientResponse response = service().path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);
			
				Assert.assertEquals("expected 200", 200, response.getStatus());
			
				multiPart.close();
				
				LOG.debug("finished writing RDF statements for RDF resource at running DB (to graph " +  resource_graph_uri + ")");
			}

}