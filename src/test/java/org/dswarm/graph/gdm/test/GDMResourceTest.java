package org.dswarm.graph.gdm.test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
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

import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class GDMResourceTest extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResourceTest.class);

	public GDMResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);
	}

	@Test
	public void writeGDMToDB() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/resources/1");
	}

	@Test
	public void testResourceTypeNodeUniqueness() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/resources/1");
		writeGDMToDBInternal("http://data.slub-dresden.de/resources/2");

		final String typeQuery = "MATCH (n) WHERE n.__NODETYPE__ = \"__TYPE_RESOURCE__\" RETURN id(n) AS node_id, n.__URI__ AS node_uri;";

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();

		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("query", typeQuery);

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		final ClientResponse response = cypher().type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final ObjectNode bodyJson = objectMapper.readValue(body, ObjectNode.class);

		Assert.assertNotNull(bodyJson);

		final JsonNode dataNode = bodyJson.get("data");

		Assert.assertNotNull(dataNode);
		Assert.assertTrue(dataNode.size() > 0);

		final Map<String, Long> resourceTypeMap = Maps.newHashMap();

		for (final JsonNode entry : dataNode) {

			final String resourceType = entry.get(1).textValue();
			final long nodeId = entry.get(0).longValue();

			if (resourceTypeMap.containsKey(resourceType)) {

				final Long existingNodeId = resourceTypeMap.get(resourceType);

				Assert.assertTrue("resource node map already contains a node for resource type '" + resourceType + "' with the id '" + existingNodeId
						+ "', but found another node with id '" + nodeId + "' for this resource type", false);
			}

			resourceTypeMap.put(resourceType, nodeId);
		}
	}

	@Test
	public void readGDMFromDBThatWasWrittenAsRDF() throws IOException {

		LOG.debug("start read test for GDM resource at " + dbType + " DB");

		writeRDFToDBInternal("http://data.slub-dresden.de/resources/1");

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final org.dswarm.graph.json.Model model = objectMapper.readValue(body, org.dswarm.graph.json.Model.class);

		LOG.debug("read '" + model.size() + "' statements");

		Assert.assertEquals("the number of statements should be 2601", 2601, model.size());

		LOG.debug("finished read test for GDM resource at " + dbType + " DB");
	}

	@Test
	public void readGDMFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read test for GDM resource at " + dbType + " DB");

		writeGDMToDBInternal("http://data.slub-dresden.de/resources/1");

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final org.dswarm.graph.json.Model model = objectMapper.readValue(body, org.dswarm.graph.json.Model.class);

		LOG.debug("read '" + model.size() + "' statements");

		Assert.assertEquals("the number of statements should be 191", 191, model.size());

		LOG.debug("finished read test for GDM resource at " + dbType + " DB");
	}

	private void writeRDFToDBInternal(final String resourceGraphURI) throws IOException {

		LOG.debug("start writing RDF statements for GDM resource at " + dbType + " DB");

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart(resourceGraphURI, MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing RDF statements for GDM resource at " + dbType + " DB");
	}

	private void writeGDMToDBInternal(final String resourceGraphURI) throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at " + dbType + " DB");

		final URL fileURL = Resources.getResource("test-mabxml.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart(resourceGraphURI, MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing GDM statements for GDM resource at " + dbType + " DB");
	}
}
