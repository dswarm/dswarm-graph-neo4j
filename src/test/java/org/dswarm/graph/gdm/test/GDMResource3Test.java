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

import org.dswarm.common.DMPStatics;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.stream.ModelParser;
import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author tgaengler
 */
public abstract class GDMResource3Test extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResource3Test.class);

	private final ObjectMapper	objectMapper;

	public GDMResource3Test(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);

		objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void mabxmlVersioningTest() throws IOException {

		final ObjectNode requestJson = getMABXMLContentSchema();

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/mabxml_dmp.gson", "versioning/mabxml_dmp2.gson",
				"http://data.slub-dresden.de/resources/1", "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType", 157, 149, false,
				Optional.<String>absent());
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void csvVersioningTest() throws IOException {

		final ObjectNode requestJson = objectMapper.createObjectNode();
		requestJson.put("record_identifier_attribute_path", "http://data.slub-dresden.de/resources/1/schema#EBL+ID");

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/Testtitel_MDunitz-US-TitleSummaryReport132968_01.csv.gson",
				"versioning/Testtitel_MDunitz-US-TitleSummaryReport132968_02.csv.gson", "http://data.slub-dresden.de/resources/2",
				"http://data.slub-dresden.de/resources/1/schema#RecordType", 36, 35, false, Optional.<String>absent());
	}

	/**
	 * no changes are between the data models
	 *
	 * @throws IOException
	 */
	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void csvVersioning01Test2() throws IOException {

		final ObjectNode requestJson = objectMapper.createObjectNode();
		requestJson.put("record_identifier_attribute_path", "http://data.slub-dresden.de/resources/1/schema#EZB-Id");

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/lic_dmp_01_v1.csv.gson", "versioning/lic_dmp_01_v2.csv.gson",
				"http://data.slub-dresden.de/resources/9", "http://data.slub-dresden.de/resources/1/schema#RecordType", 23, 23, false,
				Optional.<String>absent());
	}

	/**
	 * one record is missing in the updated data model
	 *
	 * @throws IOException
	 */
	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void csvVersioning02Test2() throws IOException {

		final ObjectNode requestJson = objectMapper.createObjectNode();
		requestJson.put("record_identifier_attribute_path", "http://data.slub-dresden.de/resources/1/schema#EZB-Id");

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/lic_dmp_02_v1.csv.gson", "versioning/lic_dmp_02_v2.csv.gson",
				"http://data.slub-dresden.de/resources/10", "http://data.slub-dresden.de/resources/1/schema#RecordType", 46, 69, true,
				Optional.of("http://data.slub-dresden.de/resources/1/schema#RecordType"));
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void csvVersioningTest2() throws IOException {

		final ObjectNode requestJson = objectMapper.createObjectNode();
		requestJson.put("record_identifier_attribute_path", "http://data.slub-dresden.de/resources/1/schema#EZB-Id");

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/lic_dmp_v1.csv.gson", "versioning/lic_dmp_v2.csv.gson",
				"http://data.slub-dresden.de/resources/8", "http://data.slub-dresden.de/resources/1/schema#RecordType", 759, 621, true,
				Optional.of("http://data.slub-dresden.de/resources/1/schema#RecordType"));
	}

	/**
	 * without content schema + makes use of record URI as record identifier
	 *
	 * @throws IOException
	 */
	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void csvVersioningTest3() throws IOException {

		readGDMFromDBThatWasWrittenAsGDM(Optional.<ObjectNode>absent(), "versioning/csv.gdm.v1.json", "versioning/csv.gdm.v2.json",
				"http://data.slub-dresden.de/resources/18", "http://data.slub-dresden.de/resources/1/schema#RecordType", 107, 113, true,
				Optional.of("http://data.slub-dresden.de/resources/1/schema#RecordType"));
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void selectedMabxmlVersioning01Test() throws IOException {

		final ObjectNode requestJson = getMABXMLContentSchema();

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/selectedOriginalsDump2011_01_v1.xml.gson",
				"versioning/selectedUpdates_01_v2.xml.gson", "http://data.slub-dresden.de/resources/3",
				"http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType", 113, 95, false, Optional.<String> absent());
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void selectedMabxmlVersioning02Test() throws IOException {

		final ObjectNode requestJson = getMABXMLContentSchema();

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/selectedOriginalsDump2011_02_v1.xml.gson",
				"versioning/selectedUpdates_02_v2.xml.gson", "http://data.slub-dresden.de/resources/4",
				"http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType", 75, 55, false, Optional.<String> absent());
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void selectedMabxmlVersioning03Test() throws IOException {

		final ObjectNode requestJson = getMABXMLContentSchema();

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/selectedOriginalsDump2011_03_v1.xml.gson",
				"versioning/selectedUpdates_03_v2.xml.gson", "http://data.slub-dresden.de/resources/5",
				"http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType", 223, 180, false, Optional.<String> absent());
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void selectedMabxmlVersioning04Test() throws IOException {

		final ObjectNode requestJson = getMABXMLContentSchema();

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/selectedOriginalsDump2011_04_v1.xml.gson",
				"versioning/selectedUpdates_04_v2.xml.gson", "http://data.slub-dresden.de/resources/6",
				"http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType", 423, 310, false, Optional.<String> absent());
	}

	// FIXME DD-809 and subtasks: skip delta calculation for now
	@Ignore
	@Test
	public void selectedMabxmlVersioningTest() throws IOException {

		final ObjectNode requestJson = getMABXMLContentSchema();

		readGDMFromDBThatWasWrittenAsGDM(Optional.of(requestJson), "versioning/selectedOriginalsDump2011_v1.xml.gson", "versioning/selectedUpdates_v2.xml.gson",
				"http://data.slub-dresden.de/resources/7", "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType", 834, 640, false,
				Optional.<String> absent());
	}

	private void readGDMFromDBThatWasWrittenAsGDM(final Optional<ObjectNode> optionalContentSchemaRequestJSON, final String resourcePathV1,
			final String resourcePathV2, final String dataModelURI, final String recordClassURI, final long statementCountCurrentVersion,
			final long statementCountV1, final boolean deprecateMissingRecords, final Optional<String> optionalRecordClassUri) throws IOException {

		LOG.debug("start read test for GDM resource at {} DB", dbType);

		writeGDMToDBInternal(resourcePathV1, dataModelURI);
		writeGDMToDBInternalWithContentSchema(resourcePathV2, dataModelURI, optionalContentSchemaRequestJSON, deprecateMissingRecords, optionalRecordClassUri);

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

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

		Assert.assertEquals("the number of statements should be " + statementCountCurrentVersion, statementCountCurrentVersion, model.size());

		// read first version
		final ObjectNode requestJson2 = objectMapper.createObjectNode();

		requestJson2.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);
		requestJson2.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		requestJson2.put(DMPStatics.VERSION_IDENTIFIER, 1);

		final String requestJsonString2 = objectMapper.writeValueAsString(requestJson2);

		// POST the request
		final ClientResponse response2 = target().path("/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, requestJsonString2);

		Assert.assertEquals("expected 200", 200, response2.getStatus());

		final InputStream actualResult2 = response2.getEntity(InputStream.class);
		final BufferedInputStream bis2 = new BufferedInputStream(actualResult2, 1024);
		final ModelParser modelParser2 = new ModelParser(bis2);
		final org.dswarm.graph.json.Model model2 = new org.dswarm.graph.json.Model();

		final Observable<Void> parseObservable2 = modelParser2.parse().map(new Func1<Resource, Void>() {

			@Override public Void call(final Resource resource) {

				model2.addResource(resource);

				return null;
			}
		});

		final Iterator<Void> iterator2 = parseObservable2.toBlocking().getIterator();

		Assert.assertTrue(iterator2.hasNext());

		while(iterator2.hasNext()) {

			iterator2.next();
		}

		bis2.close();
		actualResult2.close();

		LOG.debug("read '{}' statements", model2.size());

		Assert.assertEquals("the number of statements should be " + statementCountV1, statementCountV1, model2.size());

		LOG.debug("finished read test for GDM resource at {} DB", dbType);
	}

	private void writeGDMToDBInternalWithContentSchema(final String dataResourceFileName, final String dataModelURI,
			final Optional<ObjectNode> optionalContentSchemaRequestJSON, final boolean deprecateMissingRecords, final Optional<String> optionalRecordClassUri)
			throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource(dataResourceFileName);
		final ByteSource byteSource = Resources.asByteSource(fileURL);
		final InputStream is = byteSource.openStream();
		final BufferedInputStream bis = new BufferedInputStream(is, 1024);

		final ObjectNode metadata = objectMapper.createObjectNode();
		metadata.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		if(optionalContentSchemaRequestJSON.isPresent()) {

			metadata.set(DMPStatics.CONTENT_SCHEMA_IDENTIFIER, optionalContentSchemaRequestJSON.get());
		}

		metadata.put(DMPStatics.DEPRECATE_MISSING_RECORDS_IDENTIFIER, Boolean.valueOf(deprecateMissingRecords).toString());

		if (optionalRecordClassUri.isPresent()) {

			metadata.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, optionalRecordClassUri.get());
		}

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

	private void writeGDMToDBInternal(final String dataResourceFileName, final String dataModelURI) throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at {} DB", dbType);

		final URL fileURL = Resources.getResource(dataResourceFileName);
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

	private ObjectNode getMABXMLContentSchema() {

		final ObjectNode requestJson = objectMapper.createObjectNode();
		requestJson.put("record_identifier_attribute_path", "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#id");
		final ArrayNode keyAttributePaths = objectMapper.createArrayNode();
		keyAttributePaths.add("http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.ddb.de/professionell/mabxml/mabxml-1.xsd#nr");
		keyAttributePaths
				.add("http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.ddb.de/professionell/mabxml/mabxml-1.xsd#ind");
		requestJson.set("key_attribute_paths", keyAttributePaths);
		requestJson.put("value_attribute_path",
				"http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#feld\u001Ehttp://www.w3.org/1999/02/22-rdf-syntax-ns#value");

		return requestJson;
	}
}
