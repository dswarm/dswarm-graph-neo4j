package de.avgl.dmp.graph.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

public class RDFResourceTest {

	public static final String	MOUNT_POINT	= "/ext";
	
	@Test
	public void testPingToTestDB() throws IOException {

		final NeoServer server = CommunityServerBuilder.server().onPort(7499)
				.withThirdPartyJaxRsPackage("de.avgl.dmp.graph", RDFResourceTest.MOUNT_POINT).build();
		server.start();

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri().resolve(RDFResourceTest.MOUNT_POINT));

		// POST the request
		final ClientResponse response = service.path("/rdf/ping").get(ClientResponse.class);
		System.out.println("Response Status : " + response.getStatus());
		
		String body = response.getEntity(String.class);
		
		System.out.println("Response Status : " + body);
		
		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);
		
		server.stop();
	}

	@Test
	public void writeRDFToTestDB() throws IOException {

		final NeoServer server = CommunityServerBuilder.server().onPort(7499)
				.withThirdPartyJaxRsPackage("de.avgl.dmp.graph", RDFResourceTest.MOUNT_POINT).build();
		server.start();

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri().resolve(RDFResourceTest.MOUNT_POINT));

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/1", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service.path("/rdf").type("multipart/mixed").post(ClientResponse.class, multiPart);
		System.out.println("Response Status : " + response.getStatus());

		multiPart.close();

		server.stop();
	}
	
	@Test
	public void writeRDFToRunningDB() throws IOException {

		final Client c = Client.create();
		final WebResource service = c.resource("http://localhost:7474/graph");

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/2", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service.path("/rdf").type("multipart/mixed").post(ClientResponse.class, multiPart);
		System.out.println("Response Status : " + response.getStatus());

		multiPart.close();
	}
	
	@Test
	public void testPing() throws IOException {

		final Client c = Client.create();
		final WebResource service = c.resource("http://localhost:7474/graph");

		// POST the request
		final ClientResponse response = service.path("/rdf/ping").get(ClientResponse.class);
		System.out.println("Response Status : " + response.getStatus());
		System.out.println("Response Status : " + response.getEntity(String.class));
	}
}
