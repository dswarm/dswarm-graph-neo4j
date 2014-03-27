package de.avgl.dmp.graph.resources;

import java.io.IOException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.avgl.dmp.graph.DMPGraphException;
import de.avgl.dmp.graph.gdm.GDMReader;
import de.avgl.dmp.graph.gdm.PropertyGraphGDMReader;
import de.avgl.dmp.graph.json.Model;
import de.avgl.dmp.graph.json.util.Util;

/**
 * @author tgaengler
 */
@Path("/gdm")
public class GDMResource {

	private static final Logger									LOG	= LoggerFactory.getLogger(GDMResource.class);

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

	// @POST
	// @Path("/put")
	// @Consumes("multipart/mixed")
	// public Response writeRDF(final MultiPart multiPart, @Context final GraphDatabaseService database) {
	//
	// LOG.debug("try to process RDF statements and write them into graph db");
	//
	// final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
	// final InputStream rdfInputStream = bpe.getInputStream();
	//
	// final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);
	//
	// final Model model = ModelFactory.createDefaultModel();
	// model.read(rdfInputStream, null, "N3");
	//
	// LOG.debug("deserialized RDF statements that were serialised as Turtle or N3");
	//
	// LOG.debug("try to write RDF statements into graph db");
	//
	// final RDFHandler handler = new Neo4jRDFHandler(database, resourceGraphURI);
	// final RDFParser parser = new JenaModelParser(model);
	// parser.setRDFHandler(handler);
	// parser.parse();
	//
	// LOG.debug("finished writing " + ((Neo4jRDFHandler) handler).getCountedStatements() +
	// " RDF statements into graph db for resource graph URI '"
	// + resourceGraphURI + "'");
	//
	// return Response.ok().build();
	// }

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

		GDMResource.LOG.debug("finished reading " + model.size() + " GDM statements for resource graph uri = '" + resourceGraphUri
				+ "' and record class uri = '" + recordClassUri + "' from graph db");

		return Response.ok().entity(result).build();
	}
}
