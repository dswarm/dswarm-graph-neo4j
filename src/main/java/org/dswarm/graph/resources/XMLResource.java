package org.dswarm.graph.resources;

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

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.AttributePath;
import org.dswarm.graph.model.DMPStatics;
import org.dswarm.graph.model.util.AttributePathUtil;
import org.dswarm.graph.xml.read.PropertyGraphXMLReader;
import org.dswarm.graph.xml.read.XMLReader;

import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;

/**
 * TODO: refactor the design of the resources. this is not RESTy atm, i.e., there should be one pattern how the receive a certain data model from this unmanaged extension and depending on the Accept-Type of the request the result will be in this format (media type), i.e., "gdm", various RDF serialisation, XML, or whatever ...
 * => i.e., we don't need separate resources (endpoints) for certain descriptions (abstract formats) or representations (concrete formats, mediatypes, serialisations)
 *
 * @author tgaengler
 */
@Path("/gdm")
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
	public Response readRDF(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

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
		final Optional<AttributePath> optionalRootAttributePath = Optional
				.fromNullable(AttributePathUtil.parseAttributePathNode(rootAttributePathNode));
		final JsonNode versionNode = json.get(DMPStatics.VERSION_IDENTIFIER);
		final Integer version;

		if (versionNode != null) {

			version = versionNode.asInt();
		} else {

			version = null;
		}

		LOG.debug("try to read XML records for data model uri = '" + dataModelUri + "' and record class uri = '" + recordClassUri
				+ "' from graph db");

		final XMLReader xmlReader = new PropertyGraphXMLReader(optionalRootAttributePath, recordClassUri, dataModelUri, version, database);
		final StreamingOutput stream = new StreamingOutput() {

			@Override
			public void write(final OutputStream os) throws IOException, WebApplicationException {

				try {
					final XMLStreamWriter writer = xmlReader.read(os);

					writer.flush();
					writer.close();
				} catch (final DMPGraphException | XMLStreamException e) {

					throw new WebApplicationException(e);
				}
			}
		};

		LOG.debug(
				"finished reading '" + xmlReader.recordCount() + "' XML records for data model uri = '" + dataModelUri + "' and record class uri = '"
						+ recordClassUri + "' from graph db");

		return Response.ok(stream, MediaType.APPLICATION_XML_TYPE).build();
	}
}
