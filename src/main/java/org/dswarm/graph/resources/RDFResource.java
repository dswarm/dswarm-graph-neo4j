package org.dswarm.graph.resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.MediaTypeUtil;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.rdf.export.RDFExporter;
import org.dswarm.graph.rdf.export.RDFExporterAllData;
import org.dswarm.graph.rdf.export.RDFExporterByProvenance;
import org.dswarm.graph.rdf.parse.JenaModelParser;
import org.dswarm.graph.rdf.parse.Neo4jRDFHandler;
import org.dswarm.graph.rdf.parse.Neo4jRDFWProvenanceHandler;
import org.dswarm.graph.rdf.parse.RDFHandler;
import org.dswarm.graph.rdf.parse.RDFParser;
import org.dswarm.graph.rdf.parse.nx.NxModelParser;
import org.dswarm.graph.rdf.read.PropertyGraphRDFReader;
import org.dswarm.graph.rdf.read.RDFReader;

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

		RDFResource.LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Path("/put")
	@Consumes("multipart/mixed")
	public Response writeRDF(final MultiPart multiPart, @Context final GraphDatabaseService database) {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final Model model = ModelFactory.createDefaultModel();
		model.read(rdfInputStream, null, "N3");

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as Turtle or N3");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final RDFHandler handler = new Neo4jRDFWProvenanceHandler(database, resourceGraphURI);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		RDFResource.LOG.debug("finished writing " + ((Neo4jRDFWProvenanceHandler) handler).getCountedStatements() + " RDF statements ('"
				+ ((Neo4jRDFWProvenanceHandler) handler).getRelationShipsAdded() + "' added relationships) into graph db for resource graph URI '"
				+ resourceGraphURI + "'");

		return Response.ok().build();
	}

	@POST
	@Path("/put")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeRDF(final InputStream inputStream, @Context final GraphDatabaseService database) {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		final Model model = ModelFactory.createDefaultModel();
		model.read(inputStream, null, "N3");

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as Turtle and N3");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final RDFHandler handler = new Neo4jRDFHandler(database);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		RDFResource.LOG.debug("finished writing " + ((Neo4jRDFHandler) handler).getCountedStatements() + " RDF statements into graph db");

		return Response.ok().build();
	}

	@POST
	@Path("/putnx")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeRDFwNx(final InputStream inputStream, @Context final GraphDatabaseService database) {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		final NxParser nxParser = new NxParser(inputStream);

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as N-Triples");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final org.dswarm.graph.rdf.parse.nx.RDFHandler handler = new org.dswarm.graph.rdf.parse.nx.Neo4jRDFHandler(database);
		final org.dswarm.graph.rdf.parse.nx.RDFParser parser = new NxModelParser(nxParser);
		parser.setRDFHandler(handler);
		parser.parse();

		RDFResource.LOG.debug("finished writing " + ((org.dswarm.graph.rdf.parse.nx.Neo4jRDFHandler) handler).getCountedStatements()
				+ " RDF statements into graph db");

		return Response.ok().build();
	}

	@POST
	@Path("/putnx")
	@Consumes("multipart/mixed")
	public Response writeRDFwPROVwNx(final MultiPart multiPart, @Context final GraphDatabaseService database) {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final NxParser nxParser = new NxParser(rdfInputStream);

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as N-Triples");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final org.dswarm.graph.rdf.parse.nx.RDFHandler handler = new org.dswarm.graph.rdf.parse.nx.Neo4jRDFWProvenanceHandler(database,
				resourceGraphURI);
		final org.dswarm.graph.rdf.parse.nx.RDFParser parser = new NxModelParser(nxParser);
		parser.setRDFHandler(handler);
		parser.parse();

		RDFResource.LOG.debug("finished writing " + ((org.dswarm.graph.rdf.parse.nx.Neo4jRDFWProvenanceHandler) handler).getCountedStatements()
				+ " RDF statements into graph db for resource graph URI '" + resourceGraphURI + "'");

		return Response.ok().build();
	}

	@POST
	@Path("/get")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces("application/n-triples")
	public Response readRDF(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		RDFResource.LOG.debug("try to read RDF statements from graph db");

		final ObjectNode json;

		try {

			json = objectMapper.readValue(jsonObjectString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = "could not deserialise request JSON for read from graph DB request";

			RDFResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		final String recordClassUri = json.get("record_class_uri").asText();
		final String resourceGraphUri = json.get("resource_graph_uri").asText();

		RDFResource.LOG.debug("try to read RDF statements for resource graph uri = '" + resourceGraphUri + "' and record class uri = '"
				+ recordClassUri + "' from graph db");

		final RDFReader rdfReader = new PropertyGraphRDFReader(recordClassUri, resourceGraphUri, database);
		final Model model = rdfReader.read();

		// model.write(System.out, "N-TRIPLE");

		final StringWriter writer = new StringWriter();
		model.write(writer, "N-TRIPLE");
		final String result = writer.toString();

		RDFResource.LOG.debug("finished reading '" + model.size() + "' RDF statements ('" + rdfReader.countStatements()
				+ "' via RDF reader) for resource graph uri = '" + resourceGraphUri + "' and record class uri = '" + recordClassUri
				+ "' from graph db");

		return Response.ok().entity(result).build();
	}

	/**
	 * for triggering a download of all data models. The serialization (export) format is provided via the accept header field. If
	 * no format is provided, {@link MediaTypeUtil#N_QUADS} is used as default. In case the format is not supported a 406 response
	 * is sent.
	 * 
	 * @param database the db to export the data from
	 * @param exportFormat serialization format all data should be serialized in, injected from accept header field
	 * @return all data models serialized in exportLanguage
	 * @throws DMPGraphException in case exportFormat can not be converted to {@link MediaType}
	 */
	@GET
	// SR TODO rename to /exportall
	@Path("/getall")
	@Produces({ MediaTypeUtil.N_QUADS, MediaTypeUtil.TRIG })
	public Response exportAllRDFForDownload(@Context final GraphDatabaseService database,
			@HeaderParam("Accept") @DefaultValue(MediaTypeUtil.N_QUADS) final String exportFormat) throws DMPGraphException {

		RDFResource.LOG.debug("Start processing request to export all rdf data to format \"" + exportFormat + "\"");

		final MediaType formatType;
		try {
			formatType = MediaType.valueOf(exportFormat);
		} catch (IllegalArgumentException wrongFormat) {
			RDFResource.LOG.debug("Requested format \"" + exportFormat + "\" can not be used to create a MediaType. See exception: "
					+ wrongFormat.getLocalizedMessage());
			throw new DMPGraphException(wrongFormat.getLocalizedMessage());
		}

		// determine export language and file extension
		final Lang exportLanguage = RDFLanguages.contentTypeToLang(formatType.toString());
		final String fileExtension = exportLanguage.getFileExtensions().get(0);
		RDFResource.LOG.debug("Exporting rdf data to " + formatType.toString());

		final String result = exportAllRDFInternal(database, exportLanguage);

		RDFResource.LOG.debug("End processing request to export all rdf data to format \"" + exportFormat + "\"");

		return Response.ok(result).type(formatType.toString())
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export." + fileExtension).build();
	}

	/**
	 * trigger a download for a given data model and format. The serialization (export) format is provided via the accept header
	 * field. If no format is provided, {@link MediaTypeUtil#N_QUADS} is used as default. In case the format is not supported a
	 * 406 response is sent.
	 * 
	 * @param database the graph database
	 * @param exportFormat serialization format ({@link MediaType}) the data model should be serialized in, injected from accept
	 *            header field
	 * @param provenanceURI the data model to be exported
	 * @return a single data model, serialized in exportLanguage
	 * @throws DMPGraphException in case exportFormat can not be converted to {@link MediaType}
	 */
	@GET
	@Path("/export")
	@Produces({ MediaTypeUtil.N_QUADS, MediaTypeUtil.RDF_XML, MediaTypeUtil.TRIG, MediaTypeUtil.TURTLE, MediaTypeUtil.N3 })
	public Response exportSingleRDFForDownload(@Context final GraphDatabaseService database,
			@HeaderParam("Accept") @DefaultValue(MediaTypeUtil.N_QUADS) final String exportFormat,
			@QueryParam("provenanceuri") final String provenanceURI) throws DMPGraphException {

		RDFResource.LOG.debug("Start processing request to export rdf data for provenanceuri \"" + provenanceURI + "\" to format \"" + exportFormat
				+ "\"");

		// determine export language and file extension
		final MediaType formatType;
		try {
			formatType = MediaType.valueOf(exportFormat);
		} catch (IllegalArgumentException wrongFormat) {
			// SR TODO remove log (antipattern log+throw)
			RDFResource.LOG.debug("Requested format \"" + exportFormat + "\" can not be used to create a MediaType. See exception: "
					+ wrongFormat.getLocalizedMessage());
			throw new DMPGraphException(wrongFormat.getLocalizedMessage());
		}
		final Lang exportLanguage = RDFLanguages.contentTypeToLang(formatType.toString());
		final String fileExtension = exportLanguage.getFileExtensions().get(0);
		RDFResource.LOG.debug("Interpreting requested format \"" + exportFormat + "\" as \"" + formatType.toString() + "\"");

		// export and serialize data
		final String result = exportSingleRDFInternal(database, exportLanguage, provenanceURI);

		RDFResource.LOG.debug("End processing request to export rdf data for provenanceuri \"" + provenanceURI + "\" to format \"" + exportFormat
				+ "\"");

		return Response.ok(result).type(formatType.toString())
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export." + fileExtension).build();
	}

	/**
	 * @param database the db to export the data from
	 * @param exportLanguage the language the data should be serialized in
	 * @param provenanceURI db internal identifier of the data model
	 * @return a single data model, serialized in exportLanguage
	 */
	private String exportSingleRDFInternal(final GraphDatabaseService database, final Lang exportLanguage, final String provenanceURI) {

		RDFResource.LOG.debug("try to export all RDF statements for provenanceURI \"" + provenanceURI + "\" from graph db to format \""
				+ exportLanguage.getLabel() + "\"");

		// get data from neo4j
		final RDFExporter rdfExporter = new RDFExporterByProvenance(database, provenanceURI);
		final Dataset dataset = rdfExporter.export();
		final Model exportedModel = dataset.getNamedModel(provenanceURI);

		// serialize (export) model to exportLanguage
		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, exportedModel, exportLanguage);
		final String result = writer.toString();

		RDFResource.LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		// LOG.debug("exported result:\n" + result);

		return result;
	}

	/**
	 * @param database the db to export the data from
	 * @param exportLanguage the language all data should be serialized in
	 * @return all data models serialized in exportLanguage
	 */
	private String exportAllRDFInternal(final GraphDatabaseService database, final Lang exportLanguage) {

		RDFResource.LOG.debug("try to export all RDF statements (one graph = one data resource/model) from graph db");

		// get data from neo4j
		final RDFExporter rdfExporter = new RDFExporterAllData(database);
		final Dataset dataset = rdfExporter.export();

		// serialize (export) model to exportLanguage
		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, dataset, exportLanguage);
		final String result = writer.toString();

		RDFResource.LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		return result;
	}

}
