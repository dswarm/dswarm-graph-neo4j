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
import java.util.Iterator;

import com.hp.hpl.jena.vocabulary.RDF;
import org.junit.Assert;

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
 */
public abstract class FullRDFExportMultipleGraphsTest extends RDFExportTest {

	private static final Logger	LOG			= LoggerFactory.getLogger(FullRDFExportMultipleGraphsTest.class);
	private static final String	RDF_N3_FILE	= "dmpf_bsp1.n3";

	public FullRDFExportMultipleGraphsTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);
	}

	@Test
	public void readAllRDFFromDB() throws IOException {

		FullRDFExportMultipleGraphsTest.LOG.debug("start export all RDF statements test for RDF resource at {} DB", dbType);

		final String dataModelURI1 = "http://data.slub-dresden.de/resources/2";
		final String dataModelURI2 = "http://data.slub-dresden.de/resources/3";

		writeRDFToDBInternal(dataModelURI1, FullRDFExportMultipleGraphsTest.RDF_N3_FILE);
		writeRDFToDBInternal(dataModelURI2, FullRDFExportMultipleGraphsTest.RDF_N3_FILE);

		final ClientResponse response = service().path("/rdf/getall").accept(MediaTypeUtil.N_QUADS).get(ClientResponse.class);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		//check Content-Disposition header for correct file ending
		ExportUtils.checkContentDispositionHeader(response, ".nq");


		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body (n-quads) shouldn't be null", body);

		FullRDFExportMultipleGraphsTest.LOG.trace("Response body : {}", body);

		final InputStream stream = new ByteArrayInputStream(body.getBytes("UTF-8"));

		Assert.assertNotNull("input stream (from body) shouldn't be null", stream);

		final Dataset dataset = DatasetFactory.createMem();
		RDFDataMgr.read(dataset, stream, Lang.NQUADS);

		Assert.assertNotNull("dataset shouldn't be null", dataset);

		final long statementsInExportedRDFModel = RDFUtils.determineDatasetSize(dataset);

		FullRDFExportMultipleGraphsTest.LOG.debug("exported '{}' statements", statementsInExportedRDFModel);

		final URL fileURL = Resources.getResource(FullRDFExportMultipleGraphsTest.RDF_N3_FILE);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream inputStream = byteSource.openStream();

		final Model modelFromOriginalRDFile = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile.read(inputStream, null, "TURTLE");
		modelFromOriginalRDFile.removeAll(null, RDF.type, null);
		inputStream.close();

		final InputStream inputStream2 = byteSource.openStream();
		final Model modelFromOriginalRDFile2 = ModelFactory.createDefaultModel();
		modelFromOriginalRDFile2.read(inputStream2, null, "TURTLE");
		modelFromOriginalRDFile2.removeAll(null, RDF.type, null);
		inputStream2.close();

		final long statementsInOriginalRDFFileAfter2ndRead = modelFromOriginalRDFile.size() + modelFromOriginalRDFile2.size();

		final Dataset datasetFromSources = DatasetFactory.createMem();
		datasetFromSources.addNamedModel(dataModelURI1, modelFromOriginalRDFile);
		datasetFromSources.addNamedModel(dataModelURI2, modelFromOriginalRDFile2);

		final Iterator<String> graphURIs = datasetFromSources.listNames();

		Assert.assertNotNull("there should be some graphs in the dataset", graphURIs);

		while (graphURIs.hasNext()) {

			final String graphURI = graphURIs.next();

			Assert.assertTrue("a graph with the uri '" + graphURI + "' should be contained in the export", dataset.containsNamedModel(graphURI));

			final Model modelFromExport = dataset.getNamedModel(graphURI);

			Assert.assertNotNull("the model for graph '" + graphURI + "' shouldn't be null in the export", modelFromExport);

			final Model modelFromSource = datasetFromSources.getNamedModel(graphURI);

			Assert.assertNotNull("the model for graph '" + graphURI + "' shouldn't be null in the source", modelFromSource);

			Assert.assertTrue("the RDF model from the property graph is not isomorphic to the RDF model in the original file for graph '" + graphURI
					+ "'", modelFromSource.isIsomorphicWith(modelFromExport));
		}

		Assert.assertEquals("the number of statements should be as large as the number of statements in the model"
				+ " that read 2 times the original RDF file (" + statementsInOriginalRDFFileAfter2ndRead + ")",
				statementsInOriginalRDFFileAfter2ndRead, statementsInExportedRDFModel);

		FullRDFExportMultipleGraphsTest.LOG.debug("finished export all RDF statements test for RDF resource at {} DB", dbType);
	}

}
