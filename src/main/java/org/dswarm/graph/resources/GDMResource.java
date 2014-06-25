package org.dswarm.graph.resources;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.dswarm.graph.gdm.parse.GDMModelParser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.parse.GDMHandler;
import org.dswarm.graph.gdm.parse.GDMParser;
import org.dswarm.graph.gdm.parse.Neo4jGDMHandler;
import org.dswarm.graph.gdm.parse.Neo4jGDMWProvenanceHandler;
import org.dswarm.graph.gdm.read.GDMReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMReader;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.util.Util;

/**
 * @author tgaengler
 */
@Path("/gdm")
public class GDMResource {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResource.class);

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper	objectMapper;

	public GDMResource() {

		objectMapper = Util.getJSONObjectMapper();
	}

	@GET
	@Path("/ping")
	public String ping() {

		GDMResource.LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Path("/put")
	@Consumes("multipart/mixed")
	public Response writeGDM(final MultiPart multiPart, @Context final GraphDatabaseService database) throws DMPGraphException {

		LOG.debug("try to process GDM statements and write them into graph db");

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream gdmInputStream = bpe.getInputStream();

		if (gdmInputStream == null) {

			final String message = "input stream for write to graph DB request is null";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message);
		}

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final ObjectMapper mapper = Util.getJSONObjectMapper();

		Model model = null;
		try {
			model = mapper.readValue(gdmInputStream, Model.class);
		} catch (JsonParseException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		} catch (JsonMappingException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		} catch (IOException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		if (model == null) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message);
		}

		LOG.debug("deserialized GDM statements that were serialised as JSON");

		LOG.debug("try to write GDM statements into graph db");

		final GDMHandler handler = new Neo4jGDMWProvenanceHandler(database, resourceGraphURI);
		final GDMParser parser = new GDMModelParser(model);
		parser.setGDMHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jGDMWProvenanceHandler) handler).getCountedStatements() + " GDM statements into graph db for resource graph URI '"
				+ resourceGraphURI + "'");

		return Response.ok().build();
	}

	@POST
	@Path("/put")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeGDM(final InputStream inputStream, @Context final GraphDatabaseService database) throws DMPGraphException {

		LOG.debug("try to process GDM statements and write them into graph db");

		if (inputStream == null) {

			final String message = "input stream for write to graph DB request is null";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message);
		}

		final ObjectMapper mapper = Util.getJSONObjectMapper();

		Model model = null;
		try {
			model = mapper.readValue(inputStream, Model.class);
		} catch (JsonParseException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		} catch (JsonMappingException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		} catch (IOException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		if (model == null) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message);
		}

		LOG.debug("deserialized GDM statements that were serialised as Turtle and N3");

		LOG.debug("try to write GDM statements into graph db");

		final GDMHandler handler = new Neo4jGDMHandler(database);
		final GDMParser parser = new GDMModelParser(model);
		parser.setGDMHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jGDMHandler) handler).getCountedStatements() + " GDM statements into graph db");

		return Response.ok().build();
	}

	@POST
	@Path("/get")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response readGDM(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		GDMResource.LOG.debug("try to read GDM statements from graph db");

		final ObjectNode json;

		try {

			json = objectMapper.readValue(jsonObjectString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = "could not deserialise request JSON for read from graph DB request";

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		final String recordClassUri = json.get("record_class_uri").asText();
		final String resourceGraphUri = json.get("resource_graph_uri").asText();

		GDMResource.LOG.debug("try to read GDM statements for resource graph uri = '" + resourceGraphUri + "' and record class uri = '"
				+ recordClassUri + "' from graph db");

		final GDMReader gdmReader = new PropertyGraphGDMReader(recordClassUri, resourceGraphUri, database);
		final Model model = gdmReader.read();

		String result = null;
		try {
			result = objectMapper.writeValueAsString(model);
		} catch (final JsonProcessingException e) {

			throw new DMPGraphException("some problems occur, while processing the JSON from the GDM model", e);
		}

		GDMResource.LOG.debug("finished reading '" + model.size() + "' GDM statements ('" + gdmReader.countStatements()
				+ "' via GDM reader) for resource graph uri = '" + resourceGraphUri + "' and record class uri = '" + recordClassUri
				+ "' from graph db");

		return Response.ok().entity(result).build();
	}
}
