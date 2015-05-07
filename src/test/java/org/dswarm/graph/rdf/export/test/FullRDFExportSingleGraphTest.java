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
import java.net.URL;

import com.hp.hpl.jena.vocabulary.RDF;
import org.junit.Assert;

import org.apache.http.HttpStatus;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import org.dswarm.common.MediaTypeUtil;
import org.dswarm.common.rdf.utils.RDFUtils;
import org.dswarm.graph.test.Neo4jDBWrapper;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;

/**
 * @author polowins
 * @author tgaengler
 * @author reichert
 */
public abstract class FullRDFExportSingleGraphTest extends RDFExportTest {

	private static final Logger	LOG			= LoggerFactory.getLogger(FullRDFExportSingleGraphTest.class);

	private static final String	RDF_N3_FILE	= "dmpf_bsp1.n3";

	public FullRDFExportSingleGraphTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);
	}

	/**
	 * request to export all data in n-quads format
	 *
	 * @throws IOException
	 */
	@Test
	public void readAllRDFFromDBAcceptNquads() throws IOException {

		readAllRDFFromDBinternal(MediaTypeUtil.N_QUADS, HttpStatus.SC_OK, Lang.NQUADS, ".nq");
	}

	/**
	 * request to export all data in trig format
	 *
	 * @throws IOException
	 */
	@Test
	public void readAllRDFFromDBAcceptTriG() throws IOException {

		readAllRDFFromDBinternal(MediaTypeUtil.TRIG, HttpStatus.SC_OK, Lang.TRIG, ".trig");
	}

	/**
	 * Test the fallback to default format n-quads in case the accept header is empty
	 *
	 * @throws IOException
	 */
	@Test
	public void readAllRDFFromDBDefaultFormat() throws IOException {

		// we need to send an empty accept header. In case we omit this header field at all, the current jersey implementation
		// adds a standard header "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"
		readAllRDFFromDBinternal("", HttpStatus.SC_OK, Lang.NQUADS, ".nq");
	}

	/**
	 * request to export all data in rdf+xml format. This format is not supported, a HTTP 406 (not acceptable) response is
	 * expected.
	 *
	 * @throws IOException
	 */
	@Test
	public void readAllRDFFromDBUnsupportedFormat() throws IOException {

		readAllRDFFromDBinternal(MediaTypeUtil.RDF_XML, HttpStatus.SC_NOT_ACCEPTABLE, null, null);
	}

	/**
	 * request to export all data in a not existing format by sending some "random" accept header value. A HTTP 406 (not
	 * acceptable) response is expected.
	 *
	 * @throws IOException
	 */
	@Test
	public void readAllRDFFromDBRandomFormat() throws IOException {

		readAllRDFFromDBinternal("khlav/kalash", HttpStatus.SC_NOT_ACCEPTABLE, null, null);
	}

	/**
	 * @param requestedExportLanguage the serialization format neo4j should export the data to. (this value is used as accept
	 *            header arg to query neo4j)
	 * @param expectedHTTPResponseCode the expected HTTP status code of the response, e.g. {@link HttpStatus#SC_OK} or
	 *            {@link HttpStatus#SC_NOT_ACCEPTABLE}
	 * @param expectedExportLanguage the language the exported data is expected to be serialized in. hint: language may differ
	 *            from {@code requestedExportLanguage} to test for default values. (ignored if expectedHTTPResponseCode !=
	 *            {@link HttpStatus#SC_OK})
	 * @param expectedFileEnding the expected file ending to be received from neo4j (ignored if expectedHTTPResponseCode !=
	 *            {@link HttpStatus#SC_OK})
	 * @throws IOException
	 */
	private void readAllRDFFromDBinternal(final String requestedExportLanguage, final int expectedHTTPResponseCode, final Lang expectedExportLanguage,
			final String expectedFileEnding) throws IOException {

		FullRDFExportSingleGraphTest.LOG.debug("start export all RDF statements test for RDF resource at {} DB using a single rdf file", dbType);

		final String dataModelURI = "http://data.slub-dresden.de/resources/2";

		// prepare: write data to graph
		writeRDFToDBInternal(dataModelURI, FullRDFExportSingleGraphTest.RDF_N3_FILE);

		// request export from end point
		final ClientResponse response = service().path("/rdf/getall").accept(requestedExportLanguage).get(ClientResponse.class);

		Assert.assertEquals("expected " + expectedHTTPResponseCode, expectedHTTPResponseCode, response.getStatus());

		// in case we requested an unsupported format, stop processing here since there is no exported data to verify
		if (expectedHTTPResponseCode == HttpStatus.SC_NOT_ACCEPTABLE) {
			return;
		}

		// check Content-Disposition header for correct file ending
		ExportUtils.checkContentDispositionHeader(response, expectedFileEnding);

		// verify exported data
		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body shouldn't be null", body);

		// FullRDFExportSingleGraphTest.LOG.trace("Response body : " + body);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		Assert.assertNotNull("input stream (from body) shouldn't be null", stream);

		// read actual data set from response body
		final Dataset dataset = DatasetFactory.createMem();
		RDFDataMgr.read(dataset, stream, expectedExportLanguage);

		Assert.assertNotNull("dataset shouldn't be null", dataset);

		final long statementsInExportedRDFModel = RDFUtils.determineDatasetSize(dataset);

		FullRDFExportSingleGraphTest.LOG.debug("exported '{}' statements", statementsInExportedRDFModel);

		final URL fileURL = Resources.getResource(FullRDFExportSingleGraphTest.RDF_N3_FILE);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream inputStream = byteSource.openStream();

		final Model modelFromOriginalRDFile = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile.read(inputStream, null, "TURTLE");
		inputStream.close();
		modelFromOriginalRDFile.removeAll(null, RDF.type, null);

		final long statementsInOriginalRDFFile = modelFromOriginalRDFile.size();

		Assert.assertEquals("the number of statements should be " + statementsInOriginalRDFFile, statementsInOriginalRDFFile,
				statementsInExportedRDFModel);
		Assert.assertTrue("the received dataset should contain a graph with the data model uri '" + dataModelURI + "'",
				dataset.containsNamedModel(dataModelURI));

		final Model actualModel = dataset.getNamedModel(dataModelURI);

		Assert.assertNotNull("the graph (model) for data model uri '" + dataModelURI + "' shouldn't be null", actualModel);

		// check if statements are the "same" (isomorphic, i.e. blank nodes may have different IDs)
		Assert.assertTrue("the RDF from the property graph is not isomorphic to the RDF in the original file ",
				actualModel.isIsomorphicWith(modelFromOriginalRDFile));

		FullRDFExportSingleGraphTest.LOG.debug("finished export all RDF statements test for RDF resource at {} DB using a single rdf file", dbType);
	}

}
