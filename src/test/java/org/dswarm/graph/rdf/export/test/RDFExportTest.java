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

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

public abstract class RDFExportTest extends BasicResourceTest {

	protected static final String	TEST_RDF_FILE	= "dmpf_bsp1.n3";
	// protected static final String TEST_RDF_FILE = "turtle_untyped.ttl";
	// protected static final String TEST_RDF_FILE = "turtle_untyped_with_blanks.ttl";

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFExportTest.class);

	public RDFExportTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/rdf", dbTypeArg);
	}

	/**
	 * used as a prepare step to put some data in the graph 
	 * 
	 * @param dataModelUri the URI used as {@link GraphStatics#DATA_MODEL_PROPERTY}.
	 * @param rdfN3File the data to be stored in the graph
	 * @throws IOException
	 */
	protected void writeRDFToDBInternal(final String dataModelUri, final String rdfN3File) throws IOException {

		LOG.debug("start writing RDF statements for RDF resource at " + dbType + " DB (to graph " + dataModelUri + ")");

		final URL fileURL = Resources.getResource(rdfN3File);
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart(dataModelUri, MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing RDF statements for RDF resource at " + dbType + " DB (to graph " + dataModelUri + ")");
	}

}
