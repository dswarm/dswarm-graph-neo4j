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

import com.sun.jersey.multipart.BodyPart;
import org.dswarm.graph.delta.ContentSchema;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.gdm.parse.GDMModelParser;
import org.dswarm.graph.gdm.parse.Neo4jDeltaGDMHandler;
import org.dswarm.graph.gdm.read.PropertyGraphResourceGDMReader;
import org.dswarm.graph.gdm.work.GDMWorker;
import org.dswarm.graph.gdm.work.PropertyEnrichGDMWorker;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
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
	private final ObjectMapper				objectMapper;
	private final TestGraphDatabaseFactory	impermanentGraphDatabaseFactory;
	private static final String				IMPERMANENT_GRAPH_DATABASE_PATH	= "target/test-data/impermanent-db";

	public GDMResource() {

		objectMapper = Util.getJSONObjectMapper();
		impermanentGraphDatabaseFactory = new TestGraphDatabaseFactory();
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

		final BodyPart contentSchemaBP = multiPart.getBodyParts().get(2);
		final ContentSchema contentSchema;

		if(contentSchemaBP != null) {

			final String contentSchemaJSONString = multiPart.getBodyParts().get(2).getEntityAs(String.class);

			try {

				contentSchema = objectMapper.readValue(contentSchemaJSONString, ContentSchema.class);
			} catch (final IOException e) {

				final String message = "could not deserialise content schema JSON for write from graph DB request";

				GDMResource.LOG.debug(message);

				throw new DMPGraphException(message);
			}
		} else {

			// no content schema available for data model

			contentSchema = null;
		}

		calculateDeltaForDataModel(model, contentSchema, resourceGraphURI, database);

		final GDMHandler handler = new Neo4jGDMWProvenanceHandler(database, resourceGraphURI);
		final GDMParser parser = new GDMModelParser(model);
		parser.setGDMHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jGDMWProvenanceHandler) handler).getCountedStatements()
				+ " GDM statements into graph db for resource graph URI '" + resourceGraphURI + "'");

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

	private Model calculateDeltaForDataModel(final Model model, final ContentSchema contentSchema, final String resourceGraphURI, final GraphDatabaseService permanentDatabase) {

		final Model deltaModel = new Model();

		// calculate delta resource-wise
		for (Resource resource : model.getResources()) {

			// TODO: determine legacy resource identifier via content schema

			final String resourceURI = resource.getUri();

			// TODO: try to retrieve existing resource via legacy resource identifier in the provenance graph, i.e., build a cypher query with the help of the record identifier attribute path
			final GDMReader gdmReader = new PropertyGraphResourceGDMReader(resourceURI, resourceGraphURI, permanentDatabase);
			final Model existingResourceModel = gdmReader.read();

//			if (existingResourceModel == null) {
//
//				// take new resource model, since there was no match in the provenance graph for this resource identifier
//
//				deltaModel.addResource(resource);
//
//				// we don't need to calculate the delta, since everything is new
//
//				continue;
//			}

			final Model newResourceModel = new Model();
			newResourceModel.addResource(resource);

			calculateDeltaForResource(existingResourceModel, newResourceModel, contentSchema);
		}

		// TODO change this, i.e., return overall changeset of the datamodel

		return null;
	}

	private Model calculateDeltaForResource(final Model existingResourceModel, final Model newResourceModel, final ContentSchema contentSchema) {

		//final GraphDatabaseService existingModelDB = loadModel(existingResourceModel, IMPERMANENT_GRAPH_DATABASE_PATH + "1");
		final GraphDatabaseService newModelDB = loadModel(newResourceModel, IMPERMANENT_GRAPH_DATABASE_PATH + "2");
		//enrichModel(existingModelDB, existingResourceModel.getResources().iterator().next().getUri());
		enrichModel(newModelDB, newResourceModel.getResources().iterator().next().getUri());

		GraphDBUtil.printNodes(newModelDB);
		GraphDBUtil.printRelationships(newModelDB);

		// TODO: do delta calculation on enriched GDM models in graph

		// TODO: return a changeset model (i.e. with information for add, delete, update per triple)
		return null;
	}

	private GraphDatabaseService loadModel(final Model model, final String impermanentGraphDatabaseDir) {

		// TODO: find proper graph database settings to hold everything in-memory only
		final GraphDatabaseService impermanentDB = impermanentGraphDatabaseFactory.newImpermanentDatabaseBuilder(impermanentGraphDatabaseDir)
				.setConfig(GraphDatabaseSettings.cache_type, "strong").newGraphDatabase();

		// TODO: implement handler that enriches the GDM model with useful information for changeset detection
		final GDMHandler handler = new Neo4jDeltaGDMHandler(impermanentDB);
		final GDMParser parser = new GDMModelParser(model);
		parser.setGDMHandler(handler);
		parser.parse();

		return impermanentDB;
	}

	private void enrichModel(final GraphDatabaseService graphDB, final String resourceUri) {

		final GDMWorker worker = new PropertyEnrichGDMWorker(resourceUri, graphDB);
		worker.work();
	}


}
