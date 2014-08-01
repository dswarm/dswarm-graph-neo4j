package org.dswarm.graph.resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.AttributePath;
import org.dswarm.graph.delta.ContentSchema;
import org.dswarm.graph.delta.match.ModificationCSMatcher;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.ExactCSMatcher;
import org.dswarm.graph.delta.match.ExactCSMatcherUtil;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.delta.util.AttributePathUtil;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.gdm.parse.GDMHandler;
import org.dswarm.graph.gdm.parse.GDMModelParser;
import org.dswarm.graph.gdm.parse.GDMParser;
import org.dswarm.graph.gdm.parse.GDMResourceParser;
import org.dswarm.graph.gdm.parse.Neo4jDeltaGDMHandler;
import org.dswarm.graph.gdm.parse.Neo4jGDMHandler;
import org.dswarm.graph.gdm.parse.Neo4jGDMWProvenanceHandler;
import org.dswarm.graph.gdm.read.GDMModelReader;
import org.dswarm.graph.gdm.read.GDMResourceReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMModelReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMResourceByIDReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMResourceByURIReader;
import org.dswarm.graph.gdm.work.GDMWorker;
import org.dswarm.graph.gdm.work.PropertyEnrichGDMWorker;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;

/**
 * @author tgaengler
 */
@Path("/gdm")
public class GDMResource {

	private static final Logger				LOG								= LoggerFactory.getLogger(GDMResource.class);

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

		if (multiPart.getBodyParts().size() == 3) {

			final BodyPart contentSchemaBP = multiPart.getBodyParts().get(2);
			final ContentSchema contentSchema;

			if (contentSchemaBP != null) {

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
		}

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

		final GDMModelReader gdmReader = new PropertyGraphGDMModelReader(recordClassUri, resourceGraphUri, database);
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

	private Model calculateDeltaForDataModel(final Model model, final ContentSchema contentSchema, final String resourceGraphURI,
			final GraphDatabaseService permanentDatabase) {

		// TODO: we probably need an own changeset format instead of a model here
		final Model deltaModel = new Model();

		// calculate delta resource-wise
		for (Resource newResource : model.getResources()) {

			final String resourceURI = newResource.getUri();
			final GraphDatabaseService newResourceDB = loadResource(newResource, IMPERMANENT_GRAPH_DATABASE_PATH + "2");

			final Resource existingResource;
			final GDMResourceReader gdmReader;

			if (contentSchema.getRecordIdentifierAttributePath() != null) {

				// determine legacy resource identifier via content schema
				final String recordIdentifier = GraphDBUtil.determineRecordIdentifier(newResourceDB,
						contentSchema.getRecordIdentifierAttributePath(), newResource.getUri());

				// try to retrieve existing model via legacy record identifier
				gdmReader = new PropertyGraphGDMResourceByIDReader(recordIdentifier, contentSchema.getRecordIdentifierAttributePath(),
						resourceGraphURI, permanentDatabase);
			} else {

				// try to retrieve existing model via resource uri
				gdmReader = new PropertyGraphGDMResourceByURIReader(resourceURI, resourceGraphURI, permanentDatabase);
			}

			existingResource = gdmReader.read();

			// if (existingResourceModel == null) {
			//
			// // take new resource model, since there was no match in the provenance graph for this resource identifier
			//
			// deltaModel.addResource(resource);
			//
			// // we don't need to calculate the delta, since everything is new
			//
			// continue;
			// }

			// final Model newResourceModel = new Model();
			// newResourceModel.addResource(resource);

			calculateDeltaForResource(existingResource, newResource, newResourceDB, contentSchema);
		}

		// TODO change this, i.e., return overall changeset of the datamodel

		return null;
	}

	private Model calculateDeltaForResource(final Resource existingResource, final Resource newResource, final GraphDatabaseService newResourceDB,
			final ContentSchema contentSchema) {

		final GraphDatabaseService existingResourceDB = loadResource(existingResource, IMPERMANENT_GRAPH_DATABASE_PATH + "1");
		enrichModel(existingResourceDB, existingResource.getUri());
		enrichModel(newResourceDB, newResource.getUri());

		// GraphDBUtil.printNodes(existingResourceDB);
		// GraphDBUtil.printRelationships(existingResourceDB);
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());

		// GraphDBUtil.printNodes(newResourceDB);
		// GraphDBUtil.printRelationships(newResourceDB);
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());

		final AttributePath commonAttributePath = AttributePathUtil.determineCommonAttributePath(contentSchema);
		final Collection<CSEntity> newCSEntities = GraphDBUtil.getCSEntities(newResourceDB, newResource.getUri(), commonAttributePath, contentSchema);
		final Collection<CSEntity> existingCSEntities = GraphDBUtil.getCSEntities(existingResourceDB, existingResource.getUri(), commonAttributePath,
				contentSchema);

		// TODO: do delta calculation on enriched GDM models in graph
		// note: we can also follow a different strategy, i.e., all most exact steps first and the reduce this level, i.e., do for
		// each exact level all steps first and continue afterwards (?)
		// 1. identify exact matches for cs entities
		// 1.1 hash with key, value(s) + entity order + value(s) order => matches complete cs entities
		// TODO: keep attention to sub entities of CS values
		final ExactCSMatcher exactCSMatcher = new ExactCSMatcher(existingCSEntities, newCSEntities);
		final Collection<String> exactCSMatches = exactCSMatcher.getMatches();
		// TODO: utilise matched CS entities for path marking in graph
		final Collection<CSEntity> newExactCSMatches = ExactCSMatcherUtil.getMatches(exactCSMatches, exactCSMatcher.getNewCSEntities());
		final Collection<CSEntity> existingExactCSMatches = ExactCSMatcherUtil.getMatches(exactCSMatches, exactCSMatcher.getExistingCSEntities());
		// TODO: utilise non-matchted CS entities to continue delta calculation
		final Collection<CSEntity> newExactCSNonMatches = ExactCSMatcherUtil.getNonMatches(exactCSMatches, exactCSMatcher.getNewCSEntities());
		final Collection<CSEntity> existingExactCSNonMatches = ExactCSMatcherUtil.getNonMatches(exactCSMatches,
				exactCSMatcher.getExistingCSEntities());
		// 1.2 hash with key, value + entity order + value order => matches value entities
		// 1.3 hash with key, value + entity order => matches value entities
		// 1.4 hash with key, value => matches value entities
		// 2. identify modifications for cs entities
		// 2.1 hash with key + entity order + value order => matches value entities
		final ModificationCSMatcher modificationCSMatcher = new ModificationCSMatcher(existingExactCSNonMatches, newExactCSNonMatches);
		final Map<ValueEntity, ValueEntity> modifications = modificationCSMatcher.getModifications();
		// 2.2 hash with key + entity order => matches value entities
		// 2.3 hash with key => matches value entities
		// 3. identify exact matches of resource node-based statements or non-hierarchical sub graphs
		// 4. identify modifications of resource node-based statements or non-hierarchical sub graphs
		// 5. identify additions in new model graph
		// 6. identify removals in existing model graph
		//
		// note: mark matches or modifications after every step
		// maybe utilise confidence value for different matching approaches

		// TODO: return a changeset model (i.e. with information for add, delete, update per triple)
		return null;
	}

	private GraphDatabaseService loadResource(final Resource resource, final String impermanentGraphDatabaseDir) {

		// TODO: find proper graph database settings to hold everything in-memory only
		final GraphDatabaseService impermanentDB = impermanentGraphDatabaseFactory.newImpermanentDatabaseBuilder(impermanentGraphDatabaseDir)
				.setConfig(GraphDatabaseSettings.cache_type, "strong").newGraphDatabase();

		// TODO: implement handler that enriches the GDM resource with useful information for changeset detection
		final GDMHandler handler = new Neo4jDeltaGDMHandler(impermanentDB);

		final GDMParser parser = new GDMResourceParser(resource);
		parser.setGDMHandler(handler);
		parser.parse();

		return impermanentDB;
	}

	private void enrichModel(final GraphDatabaseService graphDB, final String resourceUri) {

		final GDMWorker worker = new PropertyEnrichGDMWorker(resourceUri, graphDB);
		worker.work();
	}

}
