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

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import org.dswarm.common.DMPStatics;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.stream.ModelParser;
import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class GDMResource4Test extends BasicResourceTest {

	private static final Logger LOG                  = LoggerFactory.getLogger(GDMResource4Test.class);
	private static final String DATA_MODEL_URI       = "http://data.slub-dresden.de/resources/1";
	private static final String MABXML_RESOURCE_GSON = "test-mabxml_w_data_model_resource.gson";

	private final ObjectMapper objectMapper;

	public GDMResource4Test(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);

		objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	@Test
	public void readGDMModelFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read GDM model test for GDM resource at {} DB", dbType);

		writeGDMToDBInternal(DATA_MODEL_URI, MABXML_RESOURCE_GSON);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType");
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, DATA_MODEL_URI);

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

		while (iterator.hasNext()) {

			iterator.next();
		}

		bis.close();
		actualResult.close();

		LOG.debug("read '{}' statements", model.size());

		Assert.assertEquals("the number of statements should be 191", 191, model.size());

		LOG.debug("finished read GDM model test for GDM resource at {} DB", dbType);
	}

	@Test
	public void readGDMRecordByURIFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read GDM record by URI test for GDM resource at {} DB", dbType);

		writeGDMToDBInternal(DATA_MODEL_URI, MABXML_RESOURCE_GSON);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordURI = "http://data.slub-dresden.de/records/e9e1fa5a-3350-43ec-bb21-6ccfa90a4497";

		requestJson.put(DMPStatics.RECORD_URI_IDENTIFIER, recordURI);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, DATA_MODEL_URI);

		final Resource actualResource = readGDMRecord(requestJson, 191);

		Assert.assertEquals(recordURI, actualResource.getUri());

		LOG.debug("finished read GDM record by URI test for GDM resource at {} DB", dbType);
	}

	@Test
	@Ignore // TODO: unignore
	public void readGDMRecordByIDFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read GDM record by ID test for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/11111";

		writeGDMToDBInternal(dataModelURI, "versioning/csv.gdm.v1.json");

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordID = "1234";
		final String recordURI = "http://data.slub-dresden.de/datamodels/DataModel-574990f5-4785-4020-b86a-9765bb084f16/records/5f7019a6-96e3-4aae-aaac-da743e2840b9";
		final String legacyRecordIdentifierAP = "http://data.slub-dresden.de/resources/1/schema#id";

		requestJson.put(DMPStatics.RECORD_ID_IDENTIFIER, recordID);
		requestJson.put(DMPStatics.LEGACY_RECORD_IDENTIFIER_ATTRIBUTE_PATH, legacyRecordIdentifierAP);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		final Resource actualResource = readGDMRecord(requestJson, 6);

		Assert.assertEquals(recordURI, actualResource.getUri());

		LOG.debug("finished read GDM record by ID test for GDM resource at {} DB", dbType);
	}

	@Test
	public void readVersionedGDMRecordByIDFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read versioned GDM record by ID test for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/2222";

		writeGDMToDBInternal(dataModelURI, "versioning/csv.gdm.v1.json");

		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";

		writeGDMToDBInternalWDeprecation(dataModelURI, "versioning/csv.gdm.v2.json", recordClassURI);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordID = "7890";
		final String recordURI = "http://data.slub-dresden.de/datamodels/DataModel-574990f5-4785-4020-b86a-9765bb084f16/records/34aa79b1-4d70-4511-a36a-4a6311300c47";
		final String legacyRecordIdentifierAP = "http://data.slub-dresden.de/resources/1/schema#id";

		requestJson.put(DMPStatics.RECORD_ID_IDENTIFIER, recordID);
		requestJson.put(DMPStatics.LEGACY_RECORD_IDENTIFIER_ATTRIBUTE_PATH, legacyRecordIdentifierAP);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		requestJson.put(DMPStatics.VERSION_IDENTIFIER, 1);

		final Resource actualResource = readGDMRecord(requestJson, 6);

		Assert.assertEquals(recordURI, actualResource.getUri());

		// should retrieve the latest version
		requestJson.remove(DMPStatics.VERSION_IDENTIFIER);

		final Resource actualResource2 = readGDMRecord(requestJson, 5);

		Assert.assertEquals(recordURI, actualResource2.getUri());

		LOG.debug("finished read versioned GDM record by ID test for GDM resource at {} DB", dbType);
	}

	@Test
	public void readVersionedGDMRecordByURIFromDBThatWasWrittenAsGDM() throws IOException {

		LOG.debug("start read versioned GDM record by URI test for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/3333";

		writeGDMToDBInternal(dataModelURI, "versioning/csv.gdm.v1.json");

		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";

		writeGDMToDBInternalWDeprecation(dataModelURI, "versioning/csv.gdm.v2.json", recordClassURI);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordURI = "http://data.slub-dresden.de/datamodels/DataModel-574990f5-4785-4020-b86a-9765bb084f16/records/34aa79b1-4d70-4511-a36a-4a6311300c47";
		final String legacyRecordIdentifierAP = "http://data.slub-dresden.de/resources/1/schema#id";

		requestJson.put(DMPStatics.RECORD_URI_IDENTIFIER, recordURI);
		requestJson.put(DMPStatics.LEGACY_RECORD_IDENTIFIER_ATTRIBUTE_PATH, legacyRecordIdentifierAP);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		requestJson.put(DMPStatics.VERSION_IDENTIFIER, 1);

		final Resource actualResource = readGDMRecord(requestJson, 6);

		Assert.assertEquals(recordURI, actualResource.getUri());

		// should retrieve the latest version
		requestJson.remove(DMPStatics.VERSION_IDENTIFIER);

		final Resource actualResource2 = readGDMRecord(requestJson, 5);

		Assert.assertEquals(recordURI, actualResource2.getUri());

		LOG.debug("finished read versioned GDM record by URI test for GDM resource at {} DB", dbType);
	}

	@Test
	public void searchGDMRecordFromDBThatWasWrittenAsGDM1() throws IOException {

		LOG.debug("start search GDM records test 1 for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/4444";

		writeGDMToDBInternal(dataModelURI, MABXML_RESOURCE_GSON);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordURI = "http://data.slub-dresden.de/records/e9e1fa5a-3350-43ec-bb21-6ccfa90a4497";
		final String keyAP = "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.w3.org/1999/02/22-rdf-syntax-ns#value";
		final String searchValue = "06978834";

		requestJson.put(DMPStatics.KEY_ATTRIBUTE_PATH_IDENTIFIER, keyAP);
		requestJson.put(DMPStatics.SEARCH_VALUE_IDENTIFIER, searchValue);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		final Model actualModel = searchGDMRecords(requestJson, 191);

		Assert.assertEquals(recordURI, actualModel.getResources().iterator().next().getUri());

		LOG.debug("finished search GDM records test 1 for GDM resource at {} DB", dbType);
	}

	@Test
	public void searchGDMRecordFromDBThatWasWrittenAsGDM2() throws IOException {

		LOG.debug("start search GDM records test 2 for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/5555";

		writeGDMToDBInternal(dataModelURI, "versioning/csv.gdm.v1.json");

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordURI = "http://data.slub-dresden.de/datamodels/DataModel-574990f5-4785-4020-b86a-9765bb084f16/records/cb31b25a-7aff-4b1d-a308-91be2fa2cc37";
		final String keyAP = "http://data.slub-dresden.de/resources/1/schema#name";
		final String searchValue = "foo";

		requestJson.put(DMPStatics.KEY_ATTRIBUTE_PATH_IDENTIFIER, keyAP);
		requestJson.put(DMPStatics.SEARCH_VALUE_IDENTIFIER, searchValue);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		final Model actualModel = searchGDMRecords(requestJson, 6);

		Assert.assertEquals(recordURI, actualModel.getResources().iterator().next().getUri());

		LOG.debug("finished search GDM records test 2 for GDM resource at {} DB", dbType);
	}

	@Test
	public void searchGDMRecordFromDBThatWasWrittenAsGDM3() throws IOException {

		LOG.debug("start search GDM records test 3 for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/6666";

		writeGDMToDBInternal(dataModelURI, "versioning/csv.gdm.v1.json");

		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";

		writeGDMToDBInternalWDeprecation(dataModelURI, "versioning/csv.gdm.v2.json", recordClassURI);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordURI = "http://data.slub-dresden.de/datamodels/DataModel-574990f5-4785-4020-b86a-9765bb084f16/records/34aa79b1-4d70-4511-a36a-4a6311300c47";
		final String keyAP = "http://data.slub-dresden.de/resources/1/schema#name";
		final String searchValue = "gluck";

		requestJson.put(DMPStatics.KEY_ATTRIBUTE_PATH_IDENTIFIER, keyAP);
		requestJson.put(DMPStatics.SEARCH_VALUE_IDENTIFIER, searchValue);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		requestJson.put(DMPStatics.VERSION_IDENTIFIER, 1);

		final Model actualModel = searchGDMRecords(requestJson, 6);

		Assert.assertEquals(recordURI, actualModel.getResources().iterator().next().getUri());

		// should retrieve the latest version
		requestJson.remove(DMPStatics.VERSION_IDENTIFIER);

		final Model actualModel2 = searchGDMRecords(requestJson, 5);

		Assert.assertEquals(recordURI, actualModel2.getResources().iterator().next().getUri());

		LOG.debug("finished search GDM records test 3 for GDM resource at {} DB", dbType);
	}

	@Test
	public void searchGDMRecordFromDBThatWasWrittenAsGDM4() throws IOException {

		LOG.debug("start search GDM records test 4 for GDM resource at {} DB", dbType);

		final String dataModelURI = "ttp://data.slub-dresden.de/resources/7777";

		writeGDMToDBInternal(dataModelURI, "versioning/csv.gdm.v1.json");

		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";

		writeGDMToDBInternalWDeprecation(dataModelURI, "versioning/csv.gdm.v2.json", recordClassURI);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		final String recordURI = "http://data.slub-dresden.de/datamodels/DataModel-574990f5-4785-4020-b86a-9765bb084f16/records/7a7990dd-2e4a-4757-85f2-7de444a0e502";
		final String keyAP = "http://data.slub-dresden.de/resources/1/schema#name";
		final String searchValue = "brumm";

		requestJson.put(DMPStatics.KEY_ATTRIBUTE_PATH_IDENTIFIER, keyAP);
		requestJson.put(DMPStatics.SEARCH_VALUE_IDENTIFIER, searchValue);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		requestJson.put(DMPStatics.VERSION_IDENTIFIER, 1);

		final Model actualModel = searchGDMRecords(requestJson, 6);

		Assert.assertEquals(recordURI, actualModel.getResources().iterator().next().getUri());

		// should retrieve the latest version
		requestJson.remove(DMPStatics.VERSION_IDENTIFIER);

		final Model actualModel2 = searchGDMRecords(requestJson, 0);

		Assert.assertNotNull(actualModel2);
		Assert.assertEquals(0, actualModel2.size());

		LOG.debug("finished search GDM records test 4 for GDM resource at {} DB", dbType);
	}

	private void writeGDMToDBInternal(final String dataModelURI, final String sourceFileName) throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource(sourceFileName);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream is = byteSource.openStream();
		final BufferedInputStream bis = new BufferedInputStream(is, 1024);

		final ObjectNode metadata = objectMapper.createObjectNode();
		metadata.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		final String requestJsonString = objectMapper.writeValueAsString(metadata);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(requestJsonString, MediaType.APPLICATION_JSON_TYPE)).bodyPart(
				new BodyPart(bis, MediaType.APPLICATION_OCTET_STREAM_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();
		bis.close();
		is.close();

		LOG.debug("finished writing GDM statements for GDM resource at {} DB", dbType);
	}

	private void writeGDMToDBInternalWDeprecation(final String dataModelURI, final String sourceFileName, final String recordClassURI)
			throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource(sourceFileName);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream is = byteSource.openStream();
		final BufferedInputStream bis = new BufferedInputStream(is, 1024);

		final ObjectNode metadata = objectMapper.createObjectNode();
		metadata.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		metadata.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);
		metadata.put(DMPStatics.DEPRECATE_MISSING_RECORDS_IDENTIFIER, Boolean.TRUE.toString());

		final String requestJsonString = objectMapper.writeValueAsString(metadata);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(requestJsonString, MediaType.APPLICATION_JSON_TYPE)).bodyPart(
				new BodyPart(bis, MediaType.APPLICATION_OCTET_STREAM_TYPE));

		// POST the request
		final ClientResponse response = target().path("/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();
		bis.close();
		is.close();

		LOG.debug("finished writing GDM statements for GDM resource at {} DB", dbType);
	}

	private Resource readGDMRecord(final JsonNode requestJson, final int expectedNumberOfStatements) throws IOException {

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/getrecord").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		final Resource resource = objectMapper.readValue(body, Resource.class);

		LOG.debug("read '{}' statements", resource.size());

		Assert.assertEquals("the number of statements should be " + expectedNumberOfStatements, expectedNumberOfStatements, resource.size());

		return resource;
	}

	private Model searchGDMRecords(final JsonNode requestJson, final int expectedNumberOfStatements) throws IOException {

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/searchrecords").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
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

		while (iterator.hasNext()) {

			iterator.next();
		}

		bis.close();
		actualResult.close();

		LOG.debug("read '{}' statements", model.size());

		Assert.assertEquals("the number of statements should be " + expectedNumberOfStatements, expectedNumberOfStatements, model.size());

		return model;
	}
}
