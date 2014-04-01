package de.avgl.dmp.graph.rdf.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public class FullRDFExportOnRunningDBTest extends RunningNeo4jTest {
	

	private static final Logger	LOG	= LoggerFactory.getLogger(FullRDFExportOnRunningDBTest.class);
	protected static final String TEST_RDF_FILE = "dmpf_bsp1.n3";
	//protected static final String TEST_RDF_FILE = "turtle_untyped.ttl";
	//protected static final String TEST_RDF_FILE = "turtle_untyped_with_blanks.ttl"; 


	public FullRDFExportOnRunningDBTest() {
		super("/ext");
	}


	@Test
	public void readAllRDFFromRunningDB() throws IOException {
		
		LOG.debug("start export all RDF test for RDF resource at running DB");
		
		writeRDFToRunningDBInternal("http://data.slub-dresden.de/resources/2");
		writeRDFToRunningDBInternal("http://data.slub-dresden.de/resources/3");

		// GET the request
		final ClientResponse response = service().path("/rdf/getall")
				.accept("application/n-triples").get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		LOG.debug("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(
				body.getBytes("UTF-8"));
		final Model model = ModelFactory.createDefaultModel();
		model.read(stream, null, "N-TRIPLE");

		LOG.debug("read '" + model.size() + "' statements");

		
		// check if statements are the "same" (isomorphic, i.e. blank nodes may have different IDs)
		final Model modelFromOriginalRDFile  = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile.read(Resources.getResource(TEST_RDF_FILE).getFile());
		//System.out.println("size after first read " + modelFromOriginalRDFile.size());
		
		long statementsInOriginalRDFFile = modelFromOriginalRDFile.size();
		
		modelFromOriginalRDFile.read(Resources.getResource(TEST_RDF_FILE).getFile());
		//System.out.println("size after second read " + modelFromOriginalRDFile.size());
		
		long statementsInOriginalRDFFileAfter2ndRead = modelFromOriginalRDFile.size();
		
		Assert.assertTrue("the RDF from the property grah is not isomorphic to the RDF in the original file ",
				model.isIsomorphicWith(modelFromOriginalRDFile));
	
		long statementsInExportedRDFModel = model.size();
		
		// this will not be equal when a file with blank nodes is imported multiple times
		/*Assert.assertEquals("the number of statements should be " + TEST_RDF_FILE_STMT_COUNT, TEST_RDF_FILE_STMT_COUNT,
		model.size());*/
		
		Assert.assertTrue("the number of statements should be as large or larger (because of isomorphic bnode-statements)"
				+ " as the number of statements in the original RDF file (" + statementsInOriginalRDFFile + "), but was " + statementsInExportedRDFModel ,
				statementsInExportedRDFModel >= statementsInOriginalRDFFile);
		
		Assert.assertEquals("the number of statements should be as large as the number of statements in the model"
				+ " that read 2 times the original RDF file (" + statementsInOriginalRDFFileAfter2ndRead + ")" ,
				statementsInExportedRDFModel, statementsInOriginalRDFFileAfter2ndRead);
		
		System.out.println("size of exported RDF model " + model.size());
		
		LOG.debug("finished export all RDF test for RDF resource at running DB");
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

	
	protected void writeRDFToRunningDBInternal(String resource_graph_uri) throws IOException {
		
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
