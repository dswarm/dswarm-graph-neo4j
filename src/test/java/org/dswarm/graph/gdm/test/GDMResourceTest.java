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
package org.dswarm.graph.gdm.test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.dswarm.common.DMPStatics;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.stream.ModelParser;
import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

import com.google.common.io.ByteSource;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author tgaengler
 */
public abstract class GDMResourceTest extends BasicResourceTest {

	private static final Logger	LOG						= LoggerFactory.getLogger(GDMResourceTest.class);

	private static final String	DEFAULT_GDM_FILE_NAME	= "test-mabxml.gson";

	private final ObjectMapper	objectMapper;

	public GDMResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);

		objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	@Test
	public void writeGDMToDB() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/resources/1", DEFAULT_GDM_FILE_NAME);
	}

	@Test
	public void writeGDMToDB2() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/datamodel/4/data", "versioning/dd-854/example_1.task.result.json");
	}

	@Test
	public void writeGDMToDB3() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/datamodel/5/data", "versioning/dd-854/example_2.task.result.json");
	}

	@Test
	public void writeGDMToDB4() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/datamodel/4/data", "versioning/dd-854/example_1.gdm.json");
		writeGDMToDBInternal("http://data.slub-dresden.de/datamodel/5/data", "versioning/dd-854/example_2.gdm.json");
		writeGDMToDBInternal("http://data.slub-dresden.de/datamodel/2/data", "versioning/dd-854/example_1.task.result.json");
		writeGDMToDBInternal("http://data.slub-dresden.de/datamodel/2/data", "versioning/dd-854/example_2.task.result.json");
	}

	@Test
	public void testResourceTypeNodeUniqueness() throws IOException {

		writeGDMToDBInternal("http://data.slub-dresden.de/resources/1", DEFAULT_GDM_FILE_NAME);
		writeGDMToDBInternal("http://data.slub-dresden.de/resources/2", DEFAULT_GDM_FILE_NAME);

		final String typeQuery = "MATCH (n:TYPE_RESOURCE) RETURN id(n) AS node_id, n.uri AS node_uri;";

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();

		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put("query", typeQuery);

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		final ClientResponse response = cypher().type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final ObjectNode bodyJson = objectMapper.readValue(body, ObjectNode.class);

		Assert.assertNotNull(bodyJson);

		final JsonNode dataNode = bodyJson.get("data");

		Assert.assertNotNull(dataNode);
		Assert.assertTrue(dataNode.size() > 0);

		final Map<String, Long> resourceTypeMap = Maps.newHashMap();

		for (final JsonNode entry : dataNode) {

			final String resourceType = entry.get(1).textValue();
			final long nodeId = entry.get(0).longValue();

			if (resourceTypeMap.containsKey(resourceType)) {

				final Long existingNodeId = resourceTypeMap.get(resourceType);

				Assert.assertTrue("resource node map already contains a node for resource type '" + resourceType + "' with the id '" + existingNodeId
						+ "', but found another node with id '" + nodeId + "' for this resource type", false);
			}

			resourceTypeMap.put(resourceType, nodeId);
		}
	}

	@Test
	public void readGDMFromDBThatWasWrittenAsRDF() throws IOException {

		LOG.debug("start read test for GDM resource at {} DB", dbType);

		final String dataModelURI = "http://data.slub-dresden.de/resources/1";

		writeRDFToDBInternal(dataModelURI);

		final String recordClassURI = "http://www.openarchives.org/OAI/2.0/recordType";
		final int numberOfStatements = 2601;

		readGDMFromDB(recordClassURI, dataModelURI, numberOfStatements, Optional.<Integer> absent());

		LOG.debug("finished read test for GDM resource at {} DB", dbType);
	}

	@Test
	public void readGDMFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read test for GDM resource at {} DB", dbType);

		final String dataModelURI = "http://data.slub-dresden.de/resources/1";

		writeGDMToDBInternal(dataModelURI, DEFAULT_GDM_FILE_NAME);

		final String recordClassURI = "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType";
		final int numberOfStatements = 152;

		readGDMFromDB(recordClassURI, dataModelURI, numberOfStatements, Optional.<Integer>absent());

		LOG.debug("finished read test for GDM resource at {} DB", dbType);
	}

	@Test
	public void testAtMostParameter() throws IOException {

		LOG.debug("start at-most parameter test for GDM resource at {} DB", dbType);

		final String dataModelURI = "http://data.slub-dresden.de/resources/2";
		final String fileName = "versioning/lic_dmp_v1.csv.gson";

		writeGDMToDBInternal(dataModelURI, fileName);

		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";
		final int numberOfStatements = 230;

		readGDMFromDB(recordClassURI, dataModelURI, numberOfStatements, Optional.of(10));

		LOG.debug("finished at-most parameter test for GDM resource at {} DB", dbType);
	}

	private void readGDMFromDB(final String recordClassURI, final String dataModelURI, final int numberOfStatements,
			final Optional<Integer> optionalAtMost) throws IOException {

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		if (optionalAtMost.isPresent()) {

			requestJson.put(DMPStatics.AT_MOST_IDENTIFIER, optionalAtMost.get());
		}

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final InputStream actualResult = response.getEntity(InputStream.class);
		final BufferedInputStream bis = new BufferedInputStream(actualResult, 1024);
		final ModelParser modelParser = new ModelParser(bis);
		final org.dswarm.graph.json.Model model = new org.dswarm.graph.json.Model();

		final Observable<Void> parseObservable = modelParser.parse().map(new Func1<Resource, Void>() {

			@Override public Void call(final Resource resource) {

				model.addResource(resource);

				return null;
			}
		});

		final Iterator<Void> iterator = parseObservable.toBlocking().getIterator();

		Assert.assertTrue(iterator.hasNext());

		while(iterator.hasNext()) {

			iterator.next();
		}

		bis.close();
		actualResult.close();

		LOG.debug("read '{}' statements", model.size());

		Assert.assertEquals("the number of statements should be " + numberOfStatements, numberOfStatements, model.size());
	}

	private void writeRDFToDBInternal(final String dataModelURI) throws IOException {

		LOG.debug("start writing RDF statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream is = byteSource.openStream();
		final BufferedInputStream bis = new BufferedInputStream(is, 1024);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(dataModelURI, MediaType.TEXT_PLAIN_TYPE)).bodyPart(new BodyPart(bis, MediaType.APPLICATION_OCTET_STREAM_TYPE));

		// POST the request
		final ClientResponse response = service().path("/rdf/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing RDF statements for GDM resource at {} DB", dbType);
	}

	private void writeGDMToDBInternal(final String dataModelURI, final String fileName) throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource(fileName);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream is = byteSource.openStream();
		final BufferedInputStream bis = new BufferedInputStream(is, 1024);

		final ObjectNode metadata = objectMapper.createObjectNode();
		metadata.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		final String requestJsonString = objectMapper.writeValueAsString(metadata);


		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(
				new BodyPart(requestJsonString, MediaType.APPLICATION_JSON_TYPE)).bodyPart(new BodyPart(bis, MediaType.APPLICATION_OCTET_STREAM_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();
		bis.close();
		is.close();

		LOG.debug("finished writing GDM statements for GDM resource at {} DB", dbType);
	}
}
