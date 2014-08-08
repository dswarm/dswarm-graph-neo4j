package org.dswarm.graph.resources;

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

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.rdf.export.RDFExporterBase;
import org.dswarm.graph.rdf.export.RDFExporterAllData;
import org.dswarm.graph.rdf.export.RDFExporterByProvenance;
import org.dswarm.graph.rdf.export.RDFExporter;
import org.dswarm.graph.rdf.parse.JenaModelParser;
import org.dswarm.graph.rdf.parse.Neo4jRDFHandler;
import org.dswarm.graph.rdf.parse.Neo4jRDFWProvenanceHandler;
import org.dswarm.graph.rdf.parse.RDFHandler;
import org.dswarm.graph.rdf.parse.RDFParser;
import org.dswarm.graph.rdf.parse.nx.NxModelParser;
import org.dswarm.graph.rdf.read.PropertyGraphRDFReader;
import org.dswarm.graph.rdf.read.RDFReader;
import org.dswarm.graph.utils.GraphUtils;

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
	 * for triggering a download of all data models in a given serialization (export) format
	 *  
	 * @param database the db to export the data from
	 * @param exportLanguage the language all data should be serialized in
	 * @return all data models serialized in exportLanguage 
	 * 
	 * @throws DMPGraphException
	 */
	@GET
	// SR TODO rename to /exportall 
	@Path("/getall")
	@Produces({ GraphUtils.N_QUADS, GraphUtils.TRIG })
	public Response exportAllRDFForDownload(@Context final GraphDatabaseService database,
			@QueryParam("format") @DefaultValue(GraphUtils.N_QUADS) String format) throws DMPGraphException {

		final MediaType formatType = getFormatType(format, GraphUtils.N_QUADS_TYPE);

		// check for accepted formats (notice "!")
		if (!(formatType.equals(GraphUtils.N_QUADS_TYPE) || formatType.equals(GraphUtils.TRIG_TYPE))) {

			throw new DMPGraphException("Unsupported media type \"" + formatType + "\", can not export data.");
		}

		final Lang exportLanguage = RDFLanguages.contentTypeToLang(formatType.toString());
		final String fileExtension = exportLanguage.getFileExtensions().get(0);

		LOG.debug("Exporting rdf data to " + formatType.toString());

		final String result = exportAllRDFInternal(database, exportLanguage);

		return Response.ok(result).type(formatType.toString())
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export." + fileExtension).build();
	}

	@GET
	@Path("/getall")
	// SR FIXME MediaType: we can not annotate to produce GraphUtils.N_QUADS here since #exportAllRDFForDownload(...) also
	// produces GraphUtils.N_QUADS. this is not allowed by Jersey, setting @Produces(GraphUtils.N_QUADS) results in an error
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response exportAllRDF(@Context final GraphDatabaseService database) throws DMPGraphException {

		final String result = exportAllRDFInternal(database, Lang.NQUADS);

		return Response.ok().entity(result).build();
	}

	/**
	 * trigger a download for a given data model and format.
	 * 
	 * @param database the graph database
	 * @param format serialization format
	 * @param provenanceURI the data model to be exported
	 * @return a single data model, serialized in exportLanguage
	 * @throws DMPGraphException
	 */
	@GET
	@Path("/export")
	@Produces({ GraphUtils.N_QUADS, GraphUtils.RDF_XML, GraphUtils.TRIG, GraphUtils.TURTLE, GraphUtils.N3 })
	public Response exportSingleRDFForDownload(@Context final GraphDatabaseService database,
			@QueryParam("format") @DefaultValue(GraphUtils.N_QUADS) String format, @QueryParam("provenanceuri") String provenanceURI)
			throws DMPGraphException {

		final MediaType formatType = getFormatType(format, GraphUtils.N_QUADS_TYPE);

		// check for accepted formats (notice "!")
		if (!(formatType.equals(GraphUtils.N_QUADS_TYPE) || formatType.equals(GraphUtils.RDF_XML_TYPE) || formatType.equals(GraphUtils.TRIG_TYPE)
				|| formatType.equals(GraphUtils.TURTLE_TYPE) || formatType.equals(GraphUtils.N3_TYPE))) {

			throw new DMPGraphException("Unsupported media type \"" + formatType + "\", can not export data.");
		}

		// do some export language processing
		final Lang exportLanguage = RDFLanguages.contentTypeToLang(formatType.toString());
		final String fileExtension = exportLanguage.getFileExtensions().get(0);
		LOG.debug("Exporting rdf data to " + formatType.toString());

		// export and serialize data
		final String result = exportSingleRDFInternal(database, exportLanguage, provenanceURI);

		return Response.ok(result).type(formatType.toString())
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export." + fileExtension).build();
	}

	/**
	 * build a {@link MediaType} from a {@link String}, assuming format consists of a type and a sub type separated by "/", e.g. "application/n-quads"
	 * 
	 * @param format String to build a {@link MediaType} from  
	 * @param defaultType default to be used if {@link MediaType} can not be built from parameter format
	 * @return 
	 */
	private MediaType getFormatType(final String format, MediaType defaultType) {

		LOG.debug("got format: \"" + format + "\"");

		final String[] formatStrings = format.split("/", 2);
		final MediaType formatType;
		if (formatStrings.length == 2) {
			formatType = new MediaType(formatStrings[0], formatStrings[1]);
		} else {
			formatType = defaultType;
		}
		return formatType;
	}

	/**
	 * @param database the db to export the data from
	 * @param exportLanguage the language the data should be serialized in 
	 * @param provenanceURI db internal identifier of the data model
	 * @return a single data model, serialized in exportLanguage 
	 */
	private String exportSingleRDFInternal(final GraphDatabaseService database, Lang exportLanguage, String provenanceURI) {

		LOG.debug("try to export all RDF statements for provenanceURI \"" + provenanceURI + "\" from graph db to format \""
				+ exportLanguage.getLabel() + "\"");

		// get data from neo4j
		final RDFExporter rdfExporter = new RDFExporterByProvenance(database, provenanceURI);
		final Dataset dataset = rdfExporter.export();
		final Model exportedModel = dataset.getNamedModel(provenanceURI);

		// serialize (export) model to exportLanguage
		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, exportedModel, exportLanguage);
		final String result = writer.toString();

		LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		// SR TODO remove?
		LOG.debug("exported result:\n" + result);

		return result;
	}
	
	/**
	 * @param database the db to export the data from
	 * @param exportLanguage the language all data should be serialized in
	 * @return all data models serialized in exportLanguage 
	 */
	private String exportAllRDFInternal(final GraphDatabaseService database, Lang exportLanguage) {

		LOG.debug("try to export all RDF statements (one graph = one data resource/model) from graph db");

		// get data from neo4j
		final RDFExporter rdfExporter = new RDFExporterAllData(database);
		final Dataset dataset = rdfExporter.export();

		// serialize (export) model to exportLanguage
		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, dataset, exportLanguage);
		final String result = writer.toString();

		LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		return result;
	}

}
