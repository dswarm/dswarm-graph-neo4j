package org.dswarm.graph.rdf.export.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import junit.framework.Assert;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;

import org.dswarm.graph.rdf.utils.RDFUtils;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author polowins
 * @author tgaengler
 */
public abstract class FullRDFExportSingleGraphTest extends RDFExportTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(FullRDFExportSingleGraphTest.class);
	
	private static final String RDF_N3_FILE = "dmpf_bsp1.n3";

	public FullRDFExportSingleGraphTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);
	}

	@Test
	public void readAllRDFFromDB() throws IOException {

		FullRDFExportSingleGraphTest.LOG.debug("start export all RDF statements test for RDF resource at " + dbType + " DB using a single rdf file");

		final String provenanceURI = "http://data.slub-dresden.de/resources/2";

		writeRDFToDBInternal(provenanceURI, RDF_N3_FILE);

		// GET the request
		final ClientResponse response = service().path("/rdf/getall").accept("application/n-quads").get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body (n-quads) shouldn't be null", body);

		FullRDFExportSingleGraphTest.LOG.trace("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		Assert.assertNotNull("input stream (from body) shouldn't be null", stream);

		final Dataset dataset = DatasetFactory.createMem();
		RDFDataMgr.read(dataset, stream, Lang.NQUADS);

		Assert.assertNotNull("dataset shouldn't be null", dataset);

		final long statementsInExportedRDFModel = RDFUtils.determineDatasetSize(dataset);

		FullRDFExportSingleGraphTest.LOG.debug("exported '" + statementsInExportedRDFModel + "' statements");

		final URL fileURL = Resources.getResource(RDF_N3_FILE);
		final InputSupplier<InputStream> inputSupplier = Resources.newInputStreamSupplier(fileURL);

		final Model modelFromOriginalRDFile = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile.read(inputSupplier.getInput(), null, "TURTLE");

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

		FullRDFExportSingleGraphTest.LOG.debug("finished export all RDF statements test for RDF resource at " + dbType + " DB using a single rdf file");
	}

}
