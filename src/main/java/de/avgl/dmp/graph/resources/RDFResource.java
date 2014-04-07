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

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.DMPGraphException;
import de.avgl.dmp.graph.rdf.export.PropertyGraphRDFExporter;
import de.avgl.dmp.graph.rdf.export.RDFExporter;
import de.avgl.dmp.graph.rdf.parse.JenaModelParser;
import de.avgl.dmp.graph.rdf.parse.Neo4jRDFHandler;
import de.avgl.dmp.graph.rdf.parse.Neo4jRDFWProvenanceHandler;
import de.avgl.dmp.graph.rdf.parse.RDFHandler;
import de.avgl.dmp.graph.rdf.parse.RDFParser;
import de.avgl.dmp.graph.rdf.read.PropertyGraphRDFReader;
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

		final RDFHandler handler = new Neo4jRDFWProvenanceHandler(database, resourceGraphURI);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jRDFWProvenanceHandler) handler).getCountedStatements() + " RDF statements into graph db for resource graph URI '"
				+ resourceGraphURI + "'");

		return Response.ok().build();
	}
	
	@POST
	@Path("/put")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeRDF(final InputStream inputStream, @Context final GraphDatabaseService database) {

		LOG.debug("try to process RDF statements and write them into graph db");

		final Model model = ModelFactory.createDefaultModel();
		model.read(inputStream, null, "N3");

		LOG.debug("deserialized RDF statements that were serialised as Turtle and N3");

		LOG.debug("try to write RDF statements into graph db");

		final RDFHandler handler = new Neo4jRDFHandler(database);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jRDFWProvenanceHandler) handler).getCountedStatements() + " RDF statements into graph db");

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

		final RDFReader rdfReader = new PropertyGraphRDFReader(recordClassUri, resourceGraphUri, database);
		final Model model = rdfReader.read();

		// model.write(System.out, "N-TRIPLE");

		final StringWriter writer = new StringWriter();
		model.write(writer, "N-TRIPLE");
		final String result = writer.toString();

		LOG.debug("finished reading '" + model.size() + "' RDF statements ('" + rdfReader.countStatements()
				+ "' via RDF reader) for resource graph uri = '" + resourceGraphUri + "' and record class uri = '" + recordClassUri
				+ "' from graph db");

		return Response.ok().entity(result).build();
	}

	@GET
	@Path("/getall")
	@Produces("application/n-quads")
	public Response exportAllRDF(@Context final GraphDatabaseService database) throws DMPGraphException {

		final String result = exportAllRDFInternal(database);

		return Response.ok().entity(result).build();
	}

	@GET
	@Path("/getall")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	// for triggering "download as ..."
	public Response exportAllRDFForDownload(@Context final GraphDatabaseService database) throws DMPGraphException {

		final String result = exportAllRDFInternal(database);

		return Response.ok().header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export.ttl").entity(result).build();
	}

	private String exportAllRDFInternal(final GraphDatabaseService database) {

		LOG.debug("try to export all RDF statements (one graph = one data resource/model) from graph db");

		final RDFExporter rdfExporter = new PropertyGraphRDFExporter(database);
		final Dataset dataset = rdfExporter.export();

		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, dataset, Lang.NQUADS);
		final String result = writer.toString();

		LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		return result;
	}
}
