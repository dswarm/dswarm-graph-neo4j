package de.avgl.dmp.graph.resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.DMPGraphException;
import de.avgl.dmp.graph.rdf.parse.JenaModelParser;
import de.avgl.dmp.graph.rdf.parse.Neo4jRDFHandler;
import de.avgl.dmp.graph.rdf.parse.RDFHandler;
import de.avgl.dmp.graph.rdf.parse.RDFParser;
import de.avgl.dmp.graph.rdf.read.PropertyGraphReader;
import de.avgl.dmp.graph.rdf.read.RDFReader;

/**
 * @author tgaengler
 */
@Path("/rdf")
public class RDFResource {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFResource.class);

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper	objectMapper;

	public RDFResource() {

		objectMapper = new ObjectMapper();
	}

	@GET
	@Path("/ping")
	public String ping() {

		LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Path("/put")
	@Consumes("multipart/mixed")
	public Response writeRDF(final MultiPart multiPart, @Context final GraphDatabaseService database) {

		LOG.debug("try to process RDF statements and write them into graph db");

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final Model model = ModelFactory.createDefaultModel();
		model.read(rdfInputStream, null, "N3");

		LOG.debug("deserialized RDF statements that were serialised as Turtle or N3");

		LOG.debug("try to write RDF statements into graph db");

		final RDFHandler handler = new Neo4jRDFHandler(database, resourceGraphURI);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jRDFHandler) handler).getCountedStatements() + " RDF statements into graph db for resource graph URI '"
				+ resourceGraphURI + "'");

		return Response.ok().build();
	}

	@POST
	@Path("/get")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces("application/n-triples")
	public Response readRDF(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		LOG.debug("try to read RDF statements from graph db");

		final ObjectNode json;

		try {

			json = objectMapper.readValue(jsonObjectString, ObjectNode.class);
		} catch (IOException e) {

			final String message = "could not deserialise request JSON for read from graph DB request";

			LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		final String recordClassUri = json.get("record_class_uri").asText();
		final String resourceGraphUri = json.get("resource_graph_uri").asText();

		LOG.debug("try to read RDF statements for resource graph uri = '" + resourceGraphUri + "' and record class uri = '" + recordClassUri
				+ "' from graph db");

		final RDFReader rdfReader = new PropertyGraphReader(recordClassUri, resourceGraphUri, database);
		final Model model = rdfReader.read();

		model.write(System.out, "N-TRIPLE");

		final StringWriter writer = new StringWriter();
		model.write(writer, "N-TRIPLE");
		final String result = writer.toString();

		LOG.debug("finished reading " + model.size() + " RDF statements for resource graph uri = '" + resourceGraphUri + "' and record class uri = '"
				+ recordClassUri + "' from graph db");

		return Response.ok().entity(result).build();
	}
}
