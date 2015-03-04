/**
 * This file is part of d:swarm graph extension. d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version. d:swarm graph extension is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with d:swarm
 * graph extension. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of d:swarm graph extension. d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version. d:swarm graph extension is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with d:swarm
 * graph extension. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * This file is part of d:swarm graph extension. d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version. d:swarm graph extension is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with d:swarm
 * graph extension. If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.xml.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import org.dswarm.common.DMPStatics;
import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Diff;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.MultiPart;

/**
 * @author tgaengler
 */
public abstract class XMLResourceTest extends BasicResourceTest {

	private static final Logger	LOG						= LoggerFactory.getLogger(XMLResourceTest.class);

	// we need PNX gson
	private static final String	DEFAULT_GDM_FILE_NAME	= "test-pnx.gson";

	public XMLResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/xml", dbTypeArg);
	}

	@Test
	public void readPNXXMLFromDB() throws IOException {

		LOG.debug("start read PNX XML test at " + dbType + " DB");

		final String dataModelURI = "http://data.slub-dresden.de/resources/1";
		final String recordClassURI = "http://www.exlibrisgroup.com/xsd/primo/primo_nm_bib#recordType";
		final String recordTag = "http://www.exlibrisgroup.com/xsd/primo/primo_nm_bib#record";

		writeGDMToDBInternal(dataModelURI, DEFAULT_GDM_FILE_NAME);

		readXMLFromDB(recordClassURI, dataModelURI, Optional.<String> absent(), Optional.of(recordTag), Optional.<Integer> absent(),
				Optional.of(DMPStatics.XML_DATA_TYPE), "test-pnx.xml");

		LOG.debug("finished read PNX XML test at " + dbType + " DB");
	}

	@Test
	public void readCSVXMLFromDB() throws IOException {

		LOG.debug("start read CSV XML test at " + dbType + " DB");

		final String dataModelURI = "http://data.slub-dresden.de/resources/2";
		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";

		writeGDMToDBInternal(dataModelURI, "versioning/Testtitel_MDunitz-US-TitleSummaryReport132968_01.csv.gson");

		readXMLFromDB(recordClassURI, dataModelURI, Optional.<String> absent(), Optional.<String> absent(), Optional.<Integer> absent(),
				Optional.<String> absent(), "Testtitel_MDunitz-US-TitleSummaryReport132968_01.csv.xml");

		LOG.debug("finished read CSV XML test at " + dbType + " DB");
	}

	@Test
	public void readMultipleRecordsCSVXMLFromDB() throws IOException {

		LOG.debug("start read multiple records CSV XML test at " + dbType + " DB");

		final String dataModelURI = "http://data.slub-dresden.de/resources/3";
		final String recordClassURI = "http://data.slub-dresden.de/resources/1/schema#RecordType";

		writeGDMToDBInternal(dataModelURI, "versioning/lic_dmp_v1.csv.gson");

		readXMLFromDB(recordClassURI, dataModelURI, Optional.<String> absent(), Optional.<String> absent(), Optional.<Integer> absent(),
				Optional.<String> absent(), "lic_dmp_v1.csv.xml");

		LOG.debug("finished read multiple records  CSV XML test at " + dbType + " DB");
	}

	@Test
	public void readXMLFromDB() throws IOException {

		LOG.debug("start read test XML test at " + dbType + " DB");

		final String dataModelURI = "http://data.slub-dresden.de/datamodel/5fddf2c5-916b-49dc-a07d-af04020c17f7/data";
		final String recordClassURI = "http://purl.org/ontology/bibo/Document";

		writeGDMToDBInternal(dataModelURI, "xml_test.gson");

		readXMLFromDB(recordClassURI, dataModelURI, Optional.<String> absent(), Optional.<String> absent(), Optional.<Integer> absent(),
				Optional.<String> absent(), "xml_test.xml");

		LOG.debug("finished read test XML test at " + dbType + " DB");
	}

	@Test
	public void readXML2FromDB() throws IOException {

		LOG.debug("start read test XML 2 test at " + dbType + " DB");

		final String dataModelURI = "http://data.slub-dresden.de/datamodel/5fddf2c5-916b-49dc-a07d-af04020c17f7/data";
		final String recordClassURI = "http://purl.org/ontology/bibo/Document";

		writeGDMToDBInternal(dataModelURI, "xml_test2.gson");

		readXMLFromDB(recordClassURI, dataModelURI, Optional.<String> absent(), Optional.<String> absent(), Optional.<Integer> absent(),
				Optional.<String> absent(), "xml_test2.xml");

		LOG.debug("finished read test XML test 2 at " + dbType + " DB");
	}

	private void readXMLFromDB(final String recordClassURI, final String dataModelURI, final Optional<String> optionalRootAttributePath,
			final Optional<String> optionalRecordTag, final Optional<Integer> optionalVersion, final Optional<String> optionalOriginalDataType,
			final String expectedFileName) throws IOException {

		final ObjectMapper objectMapper = Util.getJSONObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
		final ObjectNode requestJson = objectMapper.createObjectNode();

		requestJson.put(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, recordClassURI);
		requestJson.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);

		if (optionalRootAttributePath.isPresent()) {

			requestJson.put(DMPStatics.ROOT_ATTRIBUTE_PATH_IDENTIFIER, optionalRootAttributePath.get());
		}

		if (optionalRecordTag.isPresent()) {

			requestJson.put(DMPStatics.RECORD_TAG_IDENTIFIER, optionalRecordTag.get());
		}

		if (optionalVersion.isPresent()) {

			requestJson.put(DMPStatics.VERSION_IDENTIFIER, optionalVersion.get());
		}

		if (optionalOriginalDataType.isPresent()) {

			requestJson.put(DMPStatics.ORIGINAL_DATA_TYPE_IDENTIFIER, optionalOriginalDataType.get());
		}

		final String requestJsonString = objectMapper.writeValueAsString(requestJson);

		// POST the request
		final ClientResponse response = target().path("/get").type(MediaType.APPLICATION_JSON_TYPE).accept(MediaType.APPLICATION_XML_TYPE)
				.post(ClientResponse.class, requestJsonString);

		Assert.assertEquals("expected 200", 200, response.getStatus());
		Assert.assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());

		final String actualXML = response.getEntity(String.class);

		Assert.assertNotNull(actualXML);

		// compare result with expected result
		final URL expectedFileURL = Resources.getResource(expectedFileName);
		final String expectedXML = Resources.toString(expectedFileURL, Charsets.UTF_8);

		// do comparison: check for XML similarity
		final Diff xmlDiff = DiffBuilder.compare(Input.fromString(expectedXML)).withTest(Input.fromString(actualXML)).ignoreWhitespace()
				.checkForSimilar().build();

		Assert.assertFalse(xmlDiff.hasDifferences());
	}

	private void writeGDMToDBInternal(final String dataModelURI, final String fileName) throws IOException {

		LOG.debug("start writing GDM statements for GDM resource at " + dbType + " DB");

		final URL fileURL = Resources.getResource(fileName);
		final byte[] file = Resources.toByteArray(fileURL);

		// Construct a MultiPart with two body parts
		final MultiPart multiPart = new MultiPart();
		multiPart.bodyPart(new BodyPart(file, MediaType.APPLICATION_OCTET_STREAM_TYPE)).bodyPart(
				new BodyPart(dataModelURI, MediaType.TEXT_PLAIN_TYPE));

		// POST the request
		final ClientResponse response = service().path("/gdm/put").type("multipart/mixed").post(ClientResponse.class, multiPart);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		multiPart.close();

		LOG.debug("finished writing GDM statements for GDM resource at " + dbType + " DB");
	}
}
