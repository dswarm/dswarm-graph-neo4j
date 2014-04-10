package de.avgl.dmp.graph.rdf.nx.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public class RDFResourceOnRunningDBwPROVNxTest extends RunningNeo4jTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFResourceOnRunningDBwPROVNxTest.class);

	public RDFResourceOnRunningDBwPROVNxTest() {

		super("/ext");
	}

	@Test
	public void writeRDFToRunningDB() throws IOException {

		LOG.debug("start write test for RDF resource at running DB");

		writeRDFToRunningDBInternal();

		LOG.debug("finished write test for RDF resource at running DB");
	}

	@Test
	public void readRDFFromRunningDB() throws IOException {

		LOG.debug("start read test for RDF resource at running DB");

		writeRDFToRunningDBInternal();

		final ObjectMapper objectMapper = new ObjectMapper();
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("resource_graph_uri", "http://data.slub-dresden.de/resources/2");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = service().path("/rdf/get").type(MediaType.APPLICATION_JSON_TYPE).accept("application/n-triples")
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));
		final Model model = ModelFactory.createDefaultModel();
		model.read(stream, null, "N-TRIPLE");

		LOG.debug("read '" + model.size() + "' statements");
		
		Assert.assertEquals("the number of statements should be 2601", 2601, model.size());

		LOG.debug("finished read test for RDF resource at running DB");
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
	
	private void writeRDFToRunningDBInternal() throws IOException {
		
		LOG.debug("start writing RDF statements for RDF resource at running DB");
		
		final URL fileURL = Resources.getResource("dmpf_bsp1.nt");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart("http://data.slub-dresden.de/resources/2", MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/rdf/putnx").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();
		
		LOG.debug("finished writing RDF statements for RDF resource at running DB");
	}
}
