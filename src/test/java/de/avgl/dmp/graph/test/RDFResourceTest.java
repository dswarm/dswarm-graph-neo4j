package de.avgl.dmp.graph.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;

import com.google.common.io.Resources;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
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
				.withThirdPartyJaxRsPackage("de.avgl.dmp.graph.resources", RDFResourceTest.MOUNT_POINT).build();
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
				.withThirdPartyJaxRsPackage("de.avgl.dmp.graph.resources", RDFResourceTest.MOUNT_POINT).build();
		server.start();

		writeRDFToTestDBInternal(server);

		server.stop();
	}

	@Test
	public void readRDFFromTestDB() throws IOException {

		final NeoServer server = CommunityServerBuilder.server().onPort(7499)
				.withThirdPartyJaxRsPackage("de.avgl.dmp.graph.resources", RDFResourceTest.MOUNT_POINT).build();
		server.start();

		writeRDFToTestDBInternal(server);

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri().resolve(RDFResourceTest.MOUNT_POINT));

		final ObjectMapper objectMapper = new ObjectMapper();
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = service.path("/rdf/get").type(MediaType.APPLICATION_JSON_TYPE).accept("application/n-triples").post(ClientResponse.class, requestJsonString);
		System.out.println("Response Status : " + response.getStatus());

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		System.out.println("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));
		final Model model = ModelFactory.createDefaultModel();
		model.read(stream, null, "N-TRIPLE");

		System.out.println("number of statements = '" + model.size() + "'");

		Assert.assertEquals("the number of statements should be 2601", 2601, model.size());

		server.stop();
	}

	// enable as needed
	//@Test
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
		final ClientResponse response = service.path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);
		System.out.println("Response Status : " + response.getStatus());

		multiPart.close();

		// TODO: do clean-up in DB
	}

	// enable as needed
	//@Test
	public void readRDFFromRunningDB() throws IOException {

		writeRDFToRunningDB();

		final Client c = Client.create();
		final WebResource service = c.resource("http://localhost:7474/graph");

		final ObjectMapper objectMapper = new ObjectMapper();
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/2");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = service.path("/rdf/get").type(MediaType.APPLICATION_JSON_TYPE).accept("application/n-triples").post(ClientResponse.class, requestJsonString);
		System.out.println("Response Status : " + response.getStatus());

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		System.out.println("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));
		final Model model = ModelFactory.createDefaultModel();
		model.read(stream, null, "N-TRIPLE");

		System.out.println("number of statements = '" + model.size() + "'");

		Assert.assertEquals("the number of statements should be 2601", 2601, model.size());
	}

	// enable as needed
	//@Test
	public void testPing() throws IOException {

		final Client c = Client.create();
		final WebResource service = c.resource("http://localhost:7474/graph");

		// POST the request
		final ClientResponse response = service.path("/rdf/ping").get(ClientResponse.class);
		System.out.println("Response Status : " + response.getStatus());
		System.out.println("Response body : " + response.getEntity(String.class));
	}

	private void writeRDFToTestDBInternal(final NeoServer server) throws IOException {

		final Client c = Client.create();
		final WebResource service = c.resource(server.baseUri().resolve(RDFResourceTest.MOUNT_POINT));

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/1", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service.path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);
		System.out.println("Response Status : " + response.getStatus());

		multiPart.close();
	}
}
