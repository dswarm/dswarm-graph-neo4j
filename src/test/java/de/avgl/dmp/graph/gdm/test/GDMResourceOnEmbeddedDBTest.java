package de.avgl.dmp.graph.gdm.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.neo4j.server.NeoServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.json.util.Util;
import de.avgl.dmp.graph.test.EmbeddedNeo4jTest;

public class GDMResourceOnEmbeddedDBTest extends EmbeddedNeo4jTest {

	public GDMResourceOnEmbeddedDBTest() {

		super("/ext");
	}

	@Test
	public void testPingToTestDB() throws IOException {

		// POST the request
		final ClientResponse response = service().path("/gdm/ping").get(ClientResponse.class);
		// System.out.println("Response Status : " + response.getStatus());

		final String body = response.getEntity(String.class);

		// System.out.println("Response Status : " + body);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);
	}

	public void writeRDFToTestDB() throws IOException {

		writeRDFToTestDBInternal(server);
	}

	@Test
	public void readGDMFromTestDB() throws IOException {

		writeRDFToTestDBInternal(server);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = service().path("/gdm/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);
		// System.out.println("Response Status : " + response.getStatus());

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		// System.out.println("Response body : " + body);

		final de.avgl.dmp.graph.json.Model model = objectMapper.readValue(body, de.avgl.dmp.graph.json.Model.class);

		// System.out.println("number of statements = '" + model.size() + "'");

		Assert.assertEquals("the number of statements should be 2601", 2601, model.size());
	}

	public void writeRDFToRunningDB() throws IOException {

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/2", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);
		// System.out.println("Response Status : " + response.getStatus());

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		// TODO: do clean-up in DB
	}

	private void writeRDFToTestDBInternal(final NeoServer server) throws IOException {

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/1", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);
		// System.out.println("Response Status : " + response.getStatus());

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();
	}
}
