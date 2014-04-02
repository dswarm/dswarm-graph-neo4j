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

public class FullRDFExportMultipleGraphsOnEmbeddedDBTest extends FullRDFExportOnEmbeddedDBTest {
	
	static private final Logger	LOG	= LoggerFactory.getLogger(FullRDFExportMultipleGraphsOnEmbeddedDBTest.class);
	
	public FullRDFExportMultipleGraphsOnEmbeddedDBTest() {
		super();
	}


	@Test
	public void readAllRDFFromTestDB() throws IOException {
		
		LOG.debug("start read test for RDF resource at embedded DB");

		writeRDFToTestDBInternal(server,"http://data.slub-dresden.de/resources/2");
		writeRDFToTestDBInternal(server,"http://data.slub-dresden.de/resources/3");

		// GET the request
		final ClientResponse response = service().path("/rdf/getall")
				.accept("application/n-triples")
				.get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		LOG.debug("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(
				body.getBytes("UTF-8"));
		final Model model = ModelFactory.createDefaultModel();
		model.read(stream, null, "N-TRIPLE");

		/* write export to file for debugging */
		//model.setNsPrefixes(map); // TODO get prefix map from prefix registry
		//FileWriter fileWriter = new FileWriter("testExport.ntriples");
		//model.getWriter("TURTLE").write(model,fileWriter, null);
		//model.getWriter("N-TRIPLE").write(model,fileWriter, null);
		
		LOG.info("read '" + model.size() + "' statements");

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
		
		LOG.debug("size of exported RDF model " + model.size());

		LOG.debug("finished read test for RDF resource at embedded DB");
	}

}
