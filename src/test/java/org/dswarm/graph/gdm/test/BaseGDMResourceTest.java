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
import java.util.Optional;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import org.dswarm.common.DMPStatics;
import org.dswarm.graph.json.stream.ModelParser;
import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class BaseGDMResourceTest extends BasicResourceTest {

	private static final Logger LOG = LoggerFactory.getLogger(BaseGDMResourceTest.class);

	protected static final String DEFAULT_GDM_FILE_NAME = "test-mabxml.gson";

	protected final ObjectMapper objectMapper;

	public BaseGDMResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);

		objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	protected void writeGDMToDBInternal(final String dataModelURI, final String fileName) throws IOException {

		final ObjectNode metadata = objectMapper.createObjectNode();
		metadata.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		writeGDMToDBInternal(metadata, fileName);
	}

	protected void writeGDMToDBInternalWOVersioning(final String dataModelURI, final String fileName) throws IOException {

		final ObjectNode metadata = objectMapper.createObjectNode();
		metadata.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		metadata.put(DMPStatics.ENABLE_VERSIONING_IDENTIFIER, Boolean.FALSE.toString());

		writeGDMToDBInternal(metadata, fileName);
	}

	protected void writeGDMToDBInternal(final ObjectNode metadata, final String fileName) throws java.io.IOException {

		LOG.debug("start writing GDM statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource(fileName);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream is = byteSource.openStream();
		final BufferedInputStream bis = new BufferedInputStream(is, 1024);

		final String requestJsonString = objectMapper.writeValueAsString(metadata);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(
				new BodyPart(requestJsonString, MediaType.APPLICATION_JSON_TYPE))
				.bodyPart(new BodyPart(bis, MediaType.APPLICATION_OCTET_STREAM_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();
		bis.close();
		is.close();

		LOG.debug("finished writing GDM statements for GDM resource at {} DB", dbType);
	}

	protected void readGDMFromDB(final String recordClassURI, final String dataModelURI, final int numberOfStatements,
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

		final Observable<Void> parseObservable = modelParser.parse().map(resource1 -> {

			model.addResource(resource1);

			return null;
		});

		parseObservable.toBlocking().lastOrDefault(null);

		bis.close();
		actualResult.close();

		LOG.debug("read '{}' statements", model.size());

		Assert.assertEquals("the number of statements should be " + numberOfStatements, numberOfStatements, model.size());
	}
}
