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
package org.dswarm.graph.rdf.test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.core.MediaType;

import com.google.common.io.CharSource;
import com.hp.hpl.jena.vocabulary.RDF;
import org.junit.Assert;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class RDFResourceTest extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFResourceTest.class);

	public RDFResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/rdf", dbTypeArg);
	}

	@Test
	public void writeRDFToDB() throws IOException {

		LOG.debug("start write test for RDF resource at {} DB", dbType);

		writeRDFToDBInternal();

		LOG.debug("finished write test for RDF resource at {} DB", dbType);
	}

	@Test
	public void readRDFFromDB() throws IOException {

		LOG.debug("start read test for RDF resource at {} DB", dbType);

		writeRDFToDBInternal();

		final ObjectMapper objectMapper = new ObjectMapper();
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("record_class_uri", "http://www.openarchives.org/OAI/2.0/recordType");
		requestJson.put("data_model_uri", "http://data.slub-dresden.de/resources/1");

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/get").type(MediaType.APPLICATION_JSON_TYPE).accept("application/n-triples")
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final InputStream body = response.getEntityInputStream();

		final Model model = ModelFactory.createDefaultModel();
		model.read(body, null, "N-TRIPLE");

		LOG.debug("read '{}' statements", model.size());

		Assert.assertEquals("the number of statements should be 1935", 1935, model.size());

		LOG.debug("finished read test for RDF resource at {} DB", dbType);
	}

	private void writeRDFToDBInternal() throws IOException {

		LOG.debug("start writing RDF statements for RDF resource at {} DB", dbType);

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart("http://data.slub-dresden.de/resources/1", MediaType.TEXT_PLAIN_TYPE)).bodyPart(
				new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing RDF statements for RDF resource at {} DB", dbType);
	}

	@SuppressWarnings("unused")
	private static long countExpectedSize() throws IOException {

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final CharSource charSource = Resources.asCharSource(fileURL, StandardCharsets.UTF_8);

		final Model model = ModelFactory.createDefaultModel();
		model.read(charSource.openBufferedStream(), null, "N3");
		model.removeAll(null, RDF.type, null);

		return model.size(); // 1935
	}
}
