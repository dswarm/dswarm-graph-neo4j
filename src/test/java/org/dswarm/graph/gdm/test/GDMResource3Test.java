package org.dswarm.graph.gdm.test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.node.ArrayNode;
import junit.framework.Assert;

import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

/**
 * @author tgaengler
 */
public abstract class GDMResource3Test extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResource3Test.class);

	public GDMResource3Test(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);
	}

	//@Test
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

		Assert.assertEquals("the number of statements should be 190", 190, model.size());

		LOG.debug("finished read test for GDM resource at " + dbType + " DB");
	}

	private void writeGDMToDBInternal(final String resourceGraphURI) throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at " + dbType + " DB");

		final URL fileURL = Resources.getResource("mabxml_dmp.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();
		requestJson.put("record_identifier_attribute_path", "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#id");
		final ArrayNode keyAttributePaths = objectMapper.createArrayNode();
		keyAttributePaths.add("http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.ddb.de/professionell/mabxml/mabxml-1.xsd#nr");
		keyAttributePaths
				.add("http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.ddb.de/professionell/mabxml/mabxml-1.xsd#ind");
		requestJson.put("key_attribute_paths", keyAttributePaths);
		requestJson
				.put("value_attribute_path",
						"http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.w3.org/1999/02/22-rdf-syntax-ns#value");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE))
				.bodyPart(new BodyPart(resourceGraphURI, MediaType.TEXT_PLAIN_TYPE))
				.bodyPart(new BodyPart(requestJsonString, MediaType.APPLICATION_JSON_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing GDM statements for GDM resource at " + dbType + " DB");
	}
}
