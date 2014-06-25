package org.dswarm.graph.resources;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.rdf.export.PropertyGraphRDFExporter;
import org.dswarm.graph.rdf.export.RDFExporter;
import org.dswarm.graph.rdf.parse.JenaModelParser;
import org.dswarm.graph.rdf.parse.Neo4jRDFHandler;
import org.dswarm.graph.rdf.parse.Neo4jRDFWProvenanceHandler;
import org.dswarm.graph.rdf.parse.RDFHandler;
import org.dswarm.graph.rdf.parse.RDFParser;
import org.dswarm.graph.rdf.parse.nx.NxModelParser;
import org.dswarm.graph.rdf.read.PropertyGraphRDFReader;
import org.dswarm.graph.rdf.read.RDFReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author tgaengler
 */
@Path("/rdf")
public class RDFResource {

	private static final Logger		LOG				= LoggerFactory.getLogger(RDFResource.class);

	private static final MediaType	N_QUADS_TYPE	= new MediaType("application", "n-quads");

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper		objectMapper;

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

		LOG.debug("finished writing " + ((Neo4jRDFWProvenanceHandler) handler).getCountedStatements() + " RDF statements ('"
				+ ((Neo4jRDFWProvenanceHandler) handler).getRelationShipsAdded() + "' added relationships) into graph db for resource graph URI '"
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

		LOG.debug("finished writing " + ((Neo4jRDFHandler) handler).getCountedStatements() + " RDF statements into graph db");

		return Response.ok().build();
	}

	@POST
	@Path("/putnx")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeRDFwNx(final InputStream inputStream, @Context final GraphDatabaseService database) {

		LOG.debug("try to process RDF statements and write them into graph db");

		final NxParser nxParser = new NxParser(inputStream);

		LOG.debug("deserialized RDF statements that were serialised as N-Triples");

		LOG.debug("try to write RDF statements into graph db");

		final org.dswarm.graph.rdf.parse.nx.RDFHandler handler = new org.dswarm.graph.rdf.parse.nx.Neo4jRDFHandler(database);
		final org.dswarm.graph.rdf.parse.nx.RDFParser parser = new NxModelParser(nxParser);
		parser.setRDFHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((org.dswarm.graph.rdf.parse.nx.Neo4jRDFHandler) handler).getCountedStatements()
				+ " RDF statements into graph db");

		return Response.ok().build();
	}

	@POST
	@Path("/putnx")
	@Consumes("multipart/mixed")
	public Response writeRDFwPROVwNx(final MultiPart multiPart, @Context final GraphDatabaseService database) {

		LOG.debug("try to process RDF statements and write them into graph db");

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final NxParser nxParser = new NxParser(rdfInputStream);

		LOG.debug("deserialized RDF statements that were serialised as N-Triples");

		LOG.debug("try to write RDF statements into graph db");

		final org.dswarm.graph.rdf.parse.nx.RDFHandler handler = new org.dswarm.graph.rdf.parse.nx.Neo4jRDFWProvenanceHandler(database,
				resourceGraphURI);
		final org.dswarm.graph.rdf.parse.nx.RDFParser parser = new NxModelParser(nxParser);
		parser.setRDFHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((org.dswarm.graph.rdf.parse.nx.Neo4jRDFWProvenanceHandler) handler).getCountedStatements()
				+ " RDF statements into graph db for resource graph URI '" + resourceGraphURI + "'");

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

	/**
	 * for triggering a download
	 *
	 * @param database the graph database
	 * @throws DMPGraphException
	 */
	@GET
	@Path("/getall")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response exportAllRDFForDownload(@Context final GraphDatabaseService database,
			@QueryParam("format") @DefaultValue("application/n-quads") String format) throws DMPGraphException {

		final String[] formatStrings = format.split("/", 2);
		final MediaType formatType;
		if (formatStrings.length == 2) {
			formatType = new MediaType(formatStrings[0], formatStrings[1]);
		} else {
			formatType = N_QUADS_TYPE;
		}

		LOG.debug("Exporting rdf data into " + formatType);

		final String result = exportAllRDFInternal(database);

		return Response.ok(result, MediaType.APPLICATION_OCTET_STREAM_TYPE)
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export.ttl").build();
	}

	@GET
	@Path("/getall")
	@Produces("application/n-quads")
	public Response exportAllRDF(@Context final GraphDatabaseService database) throws DMPGraphException {

		final String result = exportAllRDFInternal(database);

		return Response.ok().entity(result).build();
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
