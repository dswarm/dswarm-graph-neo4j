/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.rdf.export.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.core.MediaType;

import com.hp.hpl.jena.vocabulary.RDF;
import org.junit.Assert;

import org.apache.http.HttpStatus;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.dswarm.common.MediaTypeUtil;
import org.dswarm.graph.rdf.export.DataModelRDFExporter;
import org.dswarm.graph.test.Neo4jDBWrapper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;

/**
 * @author reichert
 */
public abstract class PartialRDFExportTest extends RDFExportTest {

	private static final Logger	LOG							= LoggerFactory.getLogger(PartialRDFExportTest.class);

	private final String datamodel2File    = "rdfexport_datamodel2.n3";
	private final String datamodel4File    = "rdfexport_datamodel4.n3";
	private final String datamodel5File = "rdfexport_datamodel5.n3";

	private final String dataModelURI2 = "http://data.slub-dresden.de/datamodel/2/data";
	private final String dataModelURI4 = "http://data.slub-dresden.de/datamodel/4/data";
	private final String dataModelURI5 = "http://data.slub-dresden.de/datamodel/5/data";

	public PartialRDFExportTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);

	}

	@Before
	public void writeMultipleRDFToDBInternal() throws IOException {
		writeRDFToDBInternal(dataModelURI2, datamodel2File);
		writeRDFToDBInternal(dataModelURI4, datamodel4File);
		writeRDFToDBInternal(dataModelURI5, datamodel5File);
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to N3
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToN3() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.N3, dataModelURI4, HttpStatus.SC_OK, Lang.N3, datamodel4File, ".n3");
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to RDF_XML
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToRDF_XML() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.RDF_XML, dataModelURI4, HttpStatus.SC_OK, Lang.RDFXML, datamodel4File, ".rdf");
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to N_QUADS
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToN_QUADS() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.N_QUADS, dataModelURI4, HttpStatus.SC_OK, Lang.NQUADS, datamodel4File, ".nq");
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to TRIG
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToTRIG() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.TRIG, dataModelURI4, HttpStatus.SC_OK, Lang.TRIG, datamodel4File, ".trig");
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to TURTLE
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToTURTLE() throws IOException {

		exportRDFByFormatFromDBInternal(MediaTypeUtil.TURTLE, dataModelURI4, HttpStatus.SC_OK, Lang.TURTLE, datamodel4File, ".ttl");
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to the default format that is chosen in case no format is
	 * requested (i.e. empty accept parameter)
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToDefaultFormat() throws IOException {

		exportRDFByFormatFromDBInternal("", dataModelURI4, HttpStatus.SC_OK, Lang.NQUADS, datamodel4File, ".nq");
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to text/plain format. This format is not supported, a HTTP
	 * 406 (not acceptable) response is expected.
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToUnsupportedFormat() throws IOException {

		exportRDFByFormatFromDBInternal(MediaType.TEXT_PLAIN, dataModelURI4, HttpStatus.SC_NOT_ACCEPTABLE, null, null, null);
	}

	/**
	 * Export the graph identified by {@code dataModelURI4} to a not existing format by sending some "random" accept
	 * header value. A HTTP 406 (not acceptable) response is expected.
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportGraphDatamodel4FromDBToRandomFormat() throws IOException {

		exportRDFByFormatFromDBInternal("khlav/kalash", dataModelURI4, HttpStatus.SC_NOT_ACCEPTABLE, null, null, null);
	}

	/**
	 * {@link org.dswarm.graph.rdf.export.DataModelRDFExporter#export()} uses a cypher query that makes use of order by, skip and limit to request the data
	 * model in slices. This test makes sure the slicing works. <br />
	 * Additionally, the test does a self-test and fails if the resource file's model contains less statements than one cypher
	 * query returns.
	 *
	 * @throws IOException
	 */
	@Test
	public void testExportWithCypherOrderBySkipLimt() throws IOException {

		final String dataModelURI = "http://data.slub-dresden.de/resources/2";
		final String RDF_N3_FILE = "dmpf_bsp1.n3";

		// make sure the expected model has more statements than a single cypher query returns to make sure we actually test the
		// order by, skip and limit parameters by executing consecutive queries
		// hint: this is done programmatically to fail in case someone modified RDF_N3_FILE
		final Model expectedModel = RDFDataMgr.loadModel(RDF_N3_FILE);
		Assert.assertNotNull("actual model shouldn't be null", expectedModel);
		Assert.assertTrue("The cypher limit parameter must be smaller than the number of statements/relationships to be exported by this test.",
				expectedModel.size() > DataModelRDFExporter.CYPHER_LIMIT);

		writeRDFToDBInternal(dataModelURI, RDF_N3_FILE);

		exportRDFByFormatFromDBInternal(MediaTypeUtil.N3, dataModelURI, HttpStatus.SC_OK, Lang.N3, RDF_N3_FILE, ".n3");
	}

	/**
	 * TODO: add doc
	 *
	 * @param requestedExportLanguage the serialization format neo4j should export the data to. (this value is used as accept
	 *            header arg to query neo4j)
	 * @param dataModelURI identifier of the graph to export
	 * @param expectedHTTPResponseCode the expected HTTP status code of the response, e.g. {@link HttpStatus#SC_OK} or
	 *            {@link HttpStatus#SC_NOT_ACCEPTABLE}
	 * @param expectedExportLanguage the language the exported data is expected to be serialized in. hint: language may differ
	 *            from {@code requestedExportLanguage} to test for default values. (ignored if expectedHTTPResponseCode !=
	 *            {@link HttpStatus#SC_OK})
	 * @param expectedModelFile name of file containing a serialized model, this (expected) model is equal to the actual model
	 *            exported by neo4j. (ignored if expectedHTTPResponseCode != {@link HttpStatus#SC_OK})
	 * @param expectedFileEnding the expected file ending to be received from neo4j (ignored if expectedHTTPResponseCode !=
	 *            {@link HttpStatus#SC_OK})
	 * @throws IOException
	 */
	private void exportRDFByFormatFromDBInternal(final String requestedExportLanguage, final String dataModelURI,
			final int expectedHTTPResponseCode, final Lang expectedExportLanguage, final String expectedModelFile, final String expectedFileEnding)
			throws IOException {

		PartialRDFExportTest.LOG.trace("requesting export language: \"{}\"", requestedExportLanguage);

		// request export from neo4j
		final ClientResponse response = service().path("/rdf/export").queryParam("data_model_uri", dataModelURI).accept(requestedExportLanguage)
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

		PartialRDFExportTest.LOG.trace("Response body:\n{}", body);

		final InputStream inputStream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		// read actual model from response body
		final Model actualModel = ModelFactory.createDefaultModel();
		RDFDataMgr.read(actualModel, inputStream, expectedExportLanguage);

		Assert.assertNotNull("actual model shouldn't be null", actualModel);
		PartialRDFExportTest.LOG.debug("exported '{}' statements", actualModel.size());

		// read expected model from file
		final Model expectedModel = RDFDataMgr.loadModel(expectedModelFile);
		Assert.assertNotNull("expected model shouldn't be null", expectedModel);
		expectedModel.removeAll(null, RDF.type, null);

		// check if statements are the "same" (isomorphic, i.e. blank nodes may have different IDs)
		Assert.assertTrue("the RDF from the property graph is not isomorphic to the RDF in the original file ",
				actualModel.isIsomorphicWith(expectedModel));
		// end check exported data
	}

}
