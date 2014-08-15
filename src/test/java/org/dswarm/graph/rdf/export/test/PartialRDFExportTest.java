package org.dswarm.graph.rdf.export.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.core.MediaType;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;
import junit.framework.Assert;
import org.apache.http.HttpStatus;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.MediaTypeUtil;
import org.dswarm.graph.rdf.export.RDFExporterByProvenance;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author reichert
 */
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

	@Before
	public void writeMultipleRDFToDBInternal() throws IOException {
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

		exportRDFByFormatFromDBInternal(MediaTypeUtil.N3, provenanceURI_datamodel4, HttpStatus.SC_OK, Lang.N3, file_datamodel4_n3, ".n3");
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to RDF_XML
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToRDF_XML() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.RDF_XML, provenanceURI_datamodel4, HttpStatus.SC_OK, Lang.RDFXML, file_datamodel4_n3, ".rdf");
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to N_QUADS
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToN_QUADS() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.N_QUADS, provenanceURI_datamodel4, HttpStatus.SC_OK, Lang.NQUADS, file_datamodel4_n3, ".nq");
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to TRIG
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToTRIG() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.TRIG, provenanceURI_datamodel4, HttpStatus.SC_OK, Lang.TRIG, file_datamodel4_n3, ".trig");
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to TURTLE
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToTURTLE() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.TURTLE, provenanceURI_datamodel4, HttpStatus.SC_OK, Lang.TURTLE, file_datamodel4_n3, ".ttl");
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to the default format that is chosen in case no format is
	 * requested (i.e. empty accept parameter)
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToDefaultFormat() throws IOException {

		exportRDFByFormatFromDBInternal("", provenanceURI_datamodel4, HttpStatus.SC_OK, Lang.NQUADS, file_datamodel4_n3, ".nq");
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to text/plain format. This format is not supported, a HTTP
	 * 406 (not acceptable) response is expected.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToUnsupportedFormat() throws IOException {

		exportRDFByFormatFromDBInternal(MediaType.TEXT_PLAIN, provenanceURI_datamodel4, HttpStatus.SC_NOT_ACCEPTABLE, null, null, null);
	}

	/**
	 * Export the graph identified by {@code provenanceURI_datamodel4} to a not existing format by sending some "random" accept
	 * header value. A HTTP 406 (not acceptable) response is expected.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToRandomFormat() throws IOException {

		exportRDFByFormatFromDBInternal("khlav/kalash", provenanceURI_datamodel4, HttpStatus.SC_NOT_ACCEPTABLE, null, null, null);
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

		exportRDFByFormatFromDBInternal(MediaTypeUtil.N3, provenanceURI, HttpStatus.SC_OK, Lang.N3, RDF_N3_FILE, ".n3");
	}

	/**
	 * TODO: add doc
	 * 
	 * @param requestedExportLanguage the serialization format neo4j should export the data to. (this value is used as accept
	 *            header arg to query neo4j)
	 * @param provenanceURI identifier of the graph to export
	 * @param expectedHTTPResponseCode the expected HTTP status code of the response, e.g. {@link HttpStatus.SC_OK} or
	 *            {@link HttpStatus.SC_NOT_ACCEPTABLE}
	 * @param expectedExportLanguage the language the exported data is expected to be serialized in. hint: language may differ
	 *            from {@code requestedExportLanguage} to test for default values. (ignored if expectedHTTPResponseCode !=
	 *            {@link HttpStatus.SC_OK})
	 * @param expectedModelFile name of file containing a serialized model, this (expected) model is equal to the actual model
	 *            exported by neo4j. (ignored if expectedHTTPResponseCode != {@link HttpStatus.SC_OK})
	 * @param expectedFileEnding the expected file ending to be received from neo4j (ignored if expectedHTTPResponseCode !=
	 *            {@link HttpStatus.SC_OK})
	 * @throws IOException
	 */
	private void exportRDFByFormatFromDBInternal(final String requestedExportLanguage, final String provenanceURI,
			final int expectedHTTPResponseCode, final Lang expectedExportLanguage, final String expectedModelFile, final String expectedFileEnding)
			throws IOException {

		PartialRDFExportTest.LOG.trace("requesting export language: \"" + requestedExportLanguage + "\"");

		// request export from neo4j
		final ClientResponse response = service().path("/rdf/export").queryParam("provenanceuri", provenanceURI).accept(requestedExportLanguage)
				.get(ClientResponse.class);

		// check response
		Assert.assertEquals("expected " + expectedHTTPResponseCode, expectedHTTPResponseCode, response.getStatus());

		// in case we requested an unsupported format, stop processing here since there is no exported data to verify
		if (expectedHTTPResponseCode == HttpStatus.SC_NOT_ACCEPTABLE) {
			return;
		}

		// check Content-Disposition header for correct file ending
		ExportUtils.checkContentDispositionHeader(response, expectedFileEnding);

		// start check exported data
		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body shouldn't be null", body);

		PartialRDFExportTest.LOG.trace("Response body:\n" + body);

		final InputStream inputStream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		Assert.assertNotNull("input stream (from body) shouldn't be null", inputStream);

		// read actual model from response body
		final Model actualModel = ModelFactory.createDefaultModel();
		RDFDataMgr.read(actualModel, inputStream, expectedExportLanguage);

		Assert.assertNotNull("actual model shouldn't be null", actualModel);
		PartialRDFExportTest.LOG.debug("exported '" + actualModel.size() + "' statements");

		// read expected model from file
		final Model expectedModel = RDFDataMgr.loadModel(expectedModelFile);
		Assert.assertNotNull("expected model shouldn't be null", expectedModel);

		// check if statements are the "same" (isomorphic, i.e. blank nodes may have different IDs)
		Assert.assertTrue("the RDF from the property graph is not isomorphic to the RDF in the original file ",
				actualModel.isIsomorphicWith(expectedModel));
		// end check exported data
	}

}
