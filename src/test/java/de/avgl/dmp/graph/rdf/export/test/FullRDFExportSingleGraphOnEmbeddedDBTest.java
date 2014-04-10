package de.avgl.dmp.graph.rdf.export.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.Assert;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.rdf.utils.RDFUtils;

/**
 * @author polowins
 * @author tgaengler
 */
public class FullRDFExportSingleGraphOnEmbeddedDBTest extends FullRDFExportOnEmbeddedDBTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(FullRDFExportSingleGraphOnEmbeddedDBTest.class);

	public FullRDFExportSingleGraphOnEmbeddedDBTest() {

		super();
	}

	@Test
	public void readAllRDFFromTestDB() throws IOException {

		FullRDFExportSingleGraphOnEmbeddedDBTest.LOG
				.debug("start export all RDF statements test for RDF resource at embedded DB using a single rdf file");

		final String provenanceURI = "http://data.slub-dresden.de/resources/2";

		writeRDFToTestDBInternal(server, provenanceURI);

		// GET the request
		final ClientResponse response = service().path("/rdf/getall").accept("application/n-quads").get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body (n-quads) shouldn't be null", body);

		FullRDFExportSingleGraphOnEmbeddedDBTest.LOG.trace("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		Assert.assertNotNull("input stream (from body) shouldn't be null", stream);

		final Dataset dataset = DatasetFactory.createMem();
		RDFDataMgr.read(dataset, stream, Lang.NQUADS);

		Assert.assertNotNull("dataset shouldn't be null", dataset);

		final long statementsInExportedRDFModel = RDFUtils.determineDatasetSize(dataset);

		FullRDFExportSingleGraphOnEmbeddedDBTest.LOG.debug("exported '" + statementsInExportedRDFModel + "' statements");

		final Model modelFromOriginalRDFile = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile.read(Resources.getResource(FullRDFExportOnEmbeddedDBTest.TEST_RDF_FILE).getFile());

		final long statementsInOriginalRDFFile = modelFromOriginalRDFile.size();

		Assert.assertEquals("the number of statements should be " + statementsInOriginalRDFFile, statementsInOriginalRDFFile,
				statementsInExportedRDFModel);
		Assert.assertTrue("the received dataset should contain a graph with the provenance uri '" + provenanceURI + "'",
				dataset.containsNamedModel(provenanceURI));

		final Model model = dataset.getNamedModel(provenanceURI);

		Assert.assertNotNull("the graph (model) for provenance uri '" + provenanceURI + "' shouldn't be null", model);

		// check if statements are the "same" (isomorphic, i.e. blank nodes may have different IDs)
		Assert.assertTrue("the RDF from the property graph is not isomorphic to the RDF in the original file ",
				model.isIsomorphicWith(modelFromOriginalRDFile));

		FullRDFExportSingleGraphOnEmbeddedDBTest.LOG
				.debug("finished export all RDF statements test for RDF resource at embedded DB using a single rdf file");
	}

}
