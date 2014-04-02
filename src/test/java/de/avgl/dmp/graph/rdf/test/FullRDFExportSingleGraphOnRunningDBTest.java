package de.avgl.dmp.graph.rdf.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;

public class FullRDFExportSingleGraphOnRunningDBTest extends FullRDFExportOnRunningDBTest {
	
	private static final Logger	LOG	= LoggerFactory.getLogger(FullRDFExportSingleGraphOnRunningDBTest.class);
	
	public FullRDFExportSingleGraphOnRunningDBTest() {
		super("/ext");
	}


	@Test
	public void readAllRDFFromRunningDB() throws IOException {
		
		LOG.debug("start export all RDF test for RDF resource at running DB using a single rdf file");
		
		writeRDFToRunningDBInternal("http://data.slub-dresden.de/resources/2");

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

		final Model modelFromOriginalRDFile  = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile.read(Resources.getResource(TEST_RDF_FILE).getFile());
		
		long statementsInOriginalRDFFile = modelFromOriginalRDFile.size();
		long statementsInExportedRDFModel = model.size();
		
		// this will not be equal when a file with blank nodes is imported multiple times
		Assert.assertEquals("the number of statements should be " + statementsInOriginalRDFFile, statementsInExportedRDFModel,
		model.size());
		
		// check if statements are the "same" (isomorphic, i.e. blank nodes may have different IDs)
		Assert.assertTrue("the RDF from the property grah is not isomorphic to the RDF in the original file ",
				model.isIsomorphicWith(modelFromOriginalRDFile));
		
		System.out.println("size of exported RDF model " + model.size());
		
		LOG.debug("finished export all RDF test for RDF resource at running DB using a single rdf file");
	}
}
