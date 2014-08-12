package org.dswarm.graph.rdf.export.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.core.MediaType;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;
import junit.framework.Assert;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.MediaTypeUtil;
import org.dswarm.graph.rdf.export.RDFExporterByProvenance;
import org.dswarm.graph.test.Neo4jDBWrapper;

public abstract class PartialRDFExportTest extends RDFExportTest {

	private static final Logger	LOG							= LoggerFactory.getLogger(PartialRDFExportTest.class);

	private final String		file_datamodel2_n3			= "rdfexport_datamodel2.n3";
	private final String		file_datamodel4_n3			= "rdfexport_datamodel4.n3";
	private final String		file_datamodel5_n3			= "rdfexport_datamodel5.n3";

	private final String		provenanceURI_datamodel2	= "http://data.slub-dresden.de/datamodel/2/data";
	private final String		provenanceURI_datamodel4	= "http://data.slub-dresden.de/datamodel/4/data";
	private final String		provenanceURI_datamodel5	= "http://data.slub-dresden.de/datamodel/5/data";

	public PartialRDFExportTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);

	}

	private void writeMultipleRDFToDBInternal() throws IOException {
		writeRDFToDBInternal(provenanceURI_datamodel2, file_datamodel2_n3);
		writeRDFToDBInternal(provenanceURI_datamodel4, file_datamodel4_n3);
		writeRDFToDBInternal(provenanceURI_datamodel5, file_datamodel5_n3);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to N3
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToN3() throws IOException {

		final String exportLanguage = MediaTypeUtil.N3;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 200);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to RDF_XML
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToRDF_XML() throws IOException {

		final String exportLanguage = MediaTypeUtil.RDF_XML;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 200);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to N_QUADS
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToN_QUADS() throws IOException {

		final String exportLanguage = MediaTypeUtil.N_QUADS;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 200);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to TRIG
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToTRIG() throws IOException {

		final String exportLanguage = MediaTypeUtil.TRIG;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 200);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to TURTLE
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToTURTLE() throws IOException {

		final String exportLanguage = MediaTypeUtil.TURTLE;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 200);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to text/plain format. This format is not supported, a HTTP
	 * 406 (not acceptable) response is expected.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToUnsupportedFormat() throws IOException {

		final String exportLanguage = MediaType.TEXT_PLAIN;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 406);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to a not existing format by sending some "random" accept
	 * header value. A HTTP 406 (not acceptable) response is expected.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToRandomFormat() throws IOException {

		final String exportLanguage = MediaTypeUtil.N_QUADS;

		exportRDFByFormatFromDBInternal("isdfks", exportLanguage, provenanceURI_datamodel4, file_datamodel4_n3, 406);
	}

	/**
	 * {@link RDFExporterByProvenance#export()} uses a cypher query that makes use of order by, skip and limit to request the data
	 * model in slices. This test makes sure the slicing works. <br />
	 * Additionally, the test does a self-test and fails if the resource file's model contains less statements than one cypher
	 * query returns.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportWithCypherOrderBySkipLimt() throws IOException {

		final String provenanceURI = "http://data.slub-dresden.de/resources/2";
		final String RDF_N3_FILE = "dmpf_bsp1.n3";

		// make sure the expected model has more statements than a single cypher query returns to make sure we actually test the
		// order by, skip and limit parameters by executing consecutive queries
		// hint: this is done programmatically to fail in case someone modified RDF_N3_FILE
		final Model expectedModel = RDFDataMgr.loadModel(RDF_N3_FILE);
		Assert.assertNotNull("actual model shouldn't be null", expectedModel);
		Assert.assertTrue("The cypher limit parameter must be smaller than the number of statements/relationships to be exported by this test.",
				expectedModel.size() > RDFExporterByProvenance.CYPHER_LIMIT);

		writeRDFToDBInternal(provenanceURI, RDF_N3_FILE);

		final String exportLanguage = MediaTypeUtil.N3;

		exportRDFByFormatFromDBInternal(exportLanguage, exportLanguage, provenanceURI, RDF_N3_FILE, 200);
	}

	private void exportRDFByFormatFromDBInternal(final String acceptHeaderArg, final String expectedExportLanguage, final String provenanceURI,
			final String expectedModelFile, final int expectedHTTPResponseCode) throws IOException {

		writeMultipleRDFToDBInternal();

		PartialRDFExportTest.LOG.trace("requesting export language: \"" + acceptHeaderArg + "\"");

		final ClientResponse response = service().path("/rdf/export").queryParam("provenanceuri", provenanceURI).accept(acceptHeaderArg)
				.get(ClientResponse.class);

		Assert.assertEquals("expected " + expectedHTTPResponseCode, expectedHTTPResponseCode, response.getStatus());

		// in case we requested an unsupported format, stop processing here since there is no exported data to verify
		if (expectedHTTPResponseCode == 406) {
			return;
		}

		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body shouldn't be null", body);

		PartialRDFExportTest.LOG.trace("Response body:\n" + body);

		final InputStream inputStream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		Assert.assertNotNull("input stream (from body) shouldn't be null", inputStream);

		final Lang exportLang = RDFLanguages.contentTypeToLang(expectedExportLanguage);

		// read actual model from response body
		final Model actualModel = ModelFactory.createDefaultModel();
		RDFDataMgr.read(actualModel, inputStream, exportLang);

		Assert.assertNotNull("actual model shouldn't be null", actualModel);
		PartialRDFExportTest.LOG.debug("exported '" + actualModel.size() + "' statements");

		// read expected model from file
		final Model expectedModel = RDFDataMgr.loadModel(expectedModelFile);
		Assert.assertNotNull("expected model shouldn't be null", expectedModel);

		Assert.assertTrue("the RDF from the property graph is not isomorphic to the RDF in the original file ",
				actualModel.isIsomorphicWith(expectedModel));
	}

}
