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
package org.dswarm.graph.resources;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.DMPStatics;
import org.dswarm.common.model.AttributePath;
import org.dswarm.common.model.util.AttributePathUtil;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.tx.Neo4jTransactionHandler;
import org.dswarm.graph.tx.TransactionHandler;
import org.dswarm.graph.xml.read.PropertyGraphXMLReader;
import org.dswarm.graph.xml.read.XMLReader;

/**
 * TODO: refactor the design of the resources. this is not RESTy atm, i.e., there should be one pattern how the receive a certain
 * data model from this unmanaged extension and depending on the Accept-Type of the request the result will be in this format
 * (media type), i.e., "gdm", various RDF serialisation, XML, or whatever ... => i.e., we don't need separate resources
 * (endpoints) for certain descriptions (abstract formats) or representations (concrete formats, mediatypes, serialisations)
 *
 * @author tgaengler
 */
@Path("/xml")
public class XMLResource {

	private static final Logger LOG = LoggerFactory.getLogger(XMLResource.class);

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper objectMapper;

	public XMLResource() {

		objectMapper = new ObjectMapper();
	}

	@GET
	@Path("/ping")
	public String ping() {

		XMLResource.LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Path("/get")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_XML)
	public Response readXML(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		XMLResource.LOG.debug("try to read XML records from graph db");

		final ObjectNode json;

		try {

			json = objectMapper.readValue(jsonObjectString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = "could not deserialise request JSON for read from graph DB request";

			XMLResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		final String recordClassUri = json.get(DMPStatics.RECORD_CLASS_URI_IDENTIFIER).asText();
		final String dataModelUri = json.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();
		final JsonNode rootAttributePathNode = json.get(DMPStatics.ROOT_ATTRIBUTE_PATH_IDENTIFIER);
		final Optional<AttributePath> optionalRootAttributePath = Optional.fromNullable(AttributePathUtil
				.parseAttributePathNode(rootAttributePathNode));

		final Optional<String> optionalRecordTag;

		final JsonNode recordTagNode = json.get(DMPStatics.RECORD_TAG_IDENTIFIER);

		if (recordTagNode != null) {

			optionalRecordTag = Optional.fromNullable(recordTagNode.asText());
		} else {

			optionalRecordTag = Optional.absent();
		}

		final JsonNode versionNode = json.get(DMPStatics.VERSION_IDENTIFIER);
		final Integer version;

		if (versionNode != null) {

			version = versionNode.asInt();
		} else {

			version = null;
		}

		final Optional<JsonNode> optionalOriginalDataTypeNode = Optional.fromNullable(json.get(DMPStatics.ORIGINAL_DATA_TYPE_IDENTIFIER));

		final Optional<String> optionalOriginalDataType;

		if (optionalOriginalDataTypeNode.isPresent()) {

			final Optional<String> optionalOriginalDataTypeFromJSON = Optional.fromNullable(optionalOriginalDataTypeNode.get().asText());

			if (optionalOriginalDataTypeFromJSON.isPresent()) {

				optionalOriginalDataType = optionalOriginalDataTypeFromJSON;
			} else {

				optionalOriginalDataType = Optional.absent();
			}
		} else {

			optionalOriginalDataType = Optional.absent();
		}

		LOG.debug("try to read XML records for data model uri = '{}' and record class uri = '{}' from graph db", dataModelUri, recordClassUri);

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		final XMLReader xmlReader = new PropertyGraphXMLReader(optionalRootAttributePath, optionalRecordTag, recordClassUri, dataModelUri, version,
				optionalOriginalDataType, database, tx, namespaceIndex);

		final StreamingOutput stream = new StreamingOutput() {

			@Override
			public void write(final OutputStream os) throws IOException, WebApplicationException {

				try {

					final BufferedOutputStream bos = new BufferedOutputStream(os, 1024);
					final Optional<XMLStreamWriter> optionalWriter = xmlReader.read(bos);

					if (optionalWriter.isPresent()) {

						optionalWriter.get().flush();
						optionalWriter.get().close();

						LOG.debug("finished reading '{}' XML records for data model uri = '{}' and record class uri = '{}' from graph db",
								xmlReader.recordCount(), dataModelUri, recordClassUri);
					} else {

						bos.close();
						os.close();

						LOG.debug("couldn't find any XML records for data model uri = '{}' and record class uri = '{}' from graph db", dataModelUri,
								recordClassUri);
					}
				} catch (final DMPGraphException | XMLStreamException e) {

					throw new WebApplicationException(e);
				}
			}
		};

		return Response.ok(stream, MediaType.APPLICATION_XML_TYPE).build();
	}
}
