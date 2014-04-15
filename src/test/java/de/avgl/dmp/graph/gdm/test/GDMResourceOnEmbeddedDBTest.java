package de.avgl.dmp.graph.gdm.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResourceOnEmbeddedDBTest.class);

	public GDMResourceOnEmbeddedDBTest() {

		super("/ext");
	}

	@Test
	public void testPingToTestDB() throws IOException {

		LOG.debug("start ping test for GDM resource at embedded DB");

		final ClientResponse response = service().path("/gdm/ping").get(ClientResponse.class);

		final String body = response.getEntity(String.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals("expected pong", "pong", body);

		LOG.debug("finished ping test for GDM resource at embedded DB");
	}

	@Test
	public void writeGDMToTestDB() throws IOException {

		writeGDMToTestDBInternal();
	}

	@Test
	public void readGDMFromTestDBThatWasWrittenAsRDF() throws IOException {

		LOG.debug("start read test for GDM resource at embedded DB");

		writeRDFToTestDBInternal();

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = service().path("/gdm/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final de.avgl.dmp.graph.json.Model model = objectMapper.readValue(body, de.avgl.dmp.graph.json.Model.class);

		LOG.debug("read '" + model.size() + "' statements");

		Assert.assertEquals("the number of statements should be 2601", 2601, model.size());

		LOG.debug("finished read test for GDM resource at embedded DB");
	}
	
	@Test
	public void readGDMFromTestDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read test for GDM resource at embedded DB");

		writeGDMToTestDBInternal();

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = service().path("/gdm/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final de.avgl.dmp.graph.json.Model model = objectMapper.readValue(body, de.avgl.dmp.graph.json.Model.class);

		LOG.debug("read '" + model.size() + "' statements");

		Assert.assertEquals("the number of statements should be 190", 190, model.size());

		LOG.debug("finished read test for GDM resource at embedded DB");
	}

	private void writeRDFToTestDBInternal() throws IOException {

		LOG.debug("start writing RDF statements for GDM resource at embedded DB");

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/1", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing RDF statements for GDM resource at embedded DB");
	}

	private void writeGDMToTestDBInternal() throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at embedded DB");

		final URL fileURL = Resources.getResource("test-mabxml.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/1", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/gdm/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing GDM statements for GDM resource at embedded DB");
	}
}
