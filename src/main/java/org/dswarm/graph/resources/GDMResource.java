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
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.ExactCSMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactCSEntityMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactCSValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactGDMValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeModificationCSValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeModificationGDMValueMatcher;
import org.dswarm.graph.delta.match.ModificationCSValueMatcher;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.GDMValueEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.delta.match.model.util.CSEntityUtil;
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
		final ExactCSMatcher exactCSMatcher = new FirstDegreeExactCSEntityMatcher(existingCSEntities, newCSEntities);
		final Collection<String> exactCSMatches = exactCSMatcher.getMatches();
		final Collection<CSEntity> newExactCSMatches = exactCSMatcher.getMatches(exactCSMatcher.getNewCSEntities());
		final Collection<CSEntity> existingExactCSMatches = exactCSMatcher.getMatches(exactCSMatcher.getExistingCSEntities());

		// utilise matched CS entities for path marking in graph
		GraphDBUtil.markCSEntityPaths(newExactCSMatches, DeltaState.ExactMatch, newResourceDB, newResource.getUri());
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());
		GraphDBUtil.markCSEntityPaths(existingExactCSMatches, DeltaState.ExactMatch, existingResourceDB, existingResource.getUri());
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());

		final Collection<CSEntity> newExactCSNonMatches = exactCSMatcher.getNonMatches(exactCSMatcher.getNewCSEntities());
		final Collection<CSEntity> existingExactCSNonMatches = exactCSMatcher.getNonMatches(exactCSMatcher.getExistingCSEntities());
		final Collection<ValueEntity> newFirstDegreeExactCSValueNonMatches = CSEntityUtil.getValueEntities(newExactCSNonMatches);
		final Collection<ValueEntity> existingFirstDegreeExactCSValueNonMatches = CSEntityUtil.getValueEntities(existingExactCSNonMatches);
		// 1.2 hash with key, value + entity order + value order => matches value entities
		final FirstDegreeExactCSValueMatcher firstDegreeExactCSValueMatcher = new FirstDegreeExactCSValueMatcher(
				existingFirstDegreeExactCSValueNonMatches, newFirstDegreeExactCSValueNonMatches);

		final Collection<String> exactCSValueMatches = firstDegreeExactCSValueMatcher.getMatches();
		final Collection<ValueEntity> newExactCSValueMatches = firstDegreeExactCSValueMatcher.getMatches(firstDegreeExactCSValueMatcher
				.getNewValueEntities());
		final Collection<ValueEntity> existingExactCSValueMatches = firstDegreeExactCSValueMatcher.getMatches(firstDegreeExactCSValueMatcher
				.getExistingValueEntities());

		// utilise matched value entities for path marking in graph
		GraphDBUtil.markValueEntityPaths(newExactCSValueMatches, DeltaState.ExactMatch, newResourceDB, newResource.getUri());
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());
		GraphDBUtil.markValueEntityPaths(existingExactCSValueMatches, DeltaState.ExactMatch, existingResourceDB, existingResource.getUri());
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());


		final Collection<ValueEntity> newExactCSValueNonMatches = firstDegreeExactCSValueMatcher.getNonMatches(firstDegreeExactCSValueMatcher
				.getNewValueEntities());
		final Collection<ValueEntity> existingExactCSValueNonMatches = firstDegreeExactCSValueMatcher.getNonMatches(firstDegreeExactCSValueMatcher
				.getExistingValueEntities());
		// 1.3 hash with key, value + entity order => matches value entities
		// 1.4 hash with key, value => matches value entities
		// 2. identify modifications for cs entities
		// 2.1 hash with key + entity order + value order => matches value entities
		final ModificationCSValueMatcher modificationCSMatcher = new FirstDegreeModificationCSValueMatcher(existingExactCSValueNonMatches,
				newExactCSValueNonMatches);
		final Map<ValueEntity, ValueEntity> modifications = modificationCSMatcher.getModifications();
		final Collection<ValueEntity> newModificationCSMatches = modificationCSMatcher.getMatches(modificationCSMatcher.getNewValueEntities());
		final Collection<ValueEntity> existingModificationCSMatches = modificationCSMatcher.getMatches(modificationCSMatcher
				.getExistingValueEntities());

		// utilise matched value entities for path marking in graph
		GraphDBUtil.markValueEntityPaths(newModificationCSMatches, DeltaState.Modification, newResourceDB, newResource.getUri());
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());
		GraphDBUtil.markValueEntityPaths(existingModificationCSMatches, DeltaState.Modification, existingResourceDB, existingResource.getUri());
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());

		// = cs value entity additions
		final Collection<ValueEntity> newModificationCSNonMatches = modificationCSMatcher.getNonMatches(modificationCSMatcher.getNewValueEntities());
		// = cs value entity removals
		final Collection<ValueEntity> existingModificationCSNonMatches = modificationCSMatcher.getNonMatches(modificationCSMatcher
				.getExistingValueEntities());

		// utilise matched value entities for path marking in graph
		GraphDBUtil.markValueEntityPaths(newModificationCSNonMatches, DeltaState.ADDITION, newResourceDB, newResource.getUri());
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());
		GraphDBUtil.markValueEntityPaths(existingModificationCSNonMatches, DeltaState.DELETION, existingResourceDB, existingResource.getUri());
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());

		// 2.2 hash with key + entity order => matches value entities
		// 2.3 hash with key => matches value entities
		// 3. identify exact matches of resource node-based statements or non-hierarchical sub graphs
		final Collection<ValueEntity> newFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(newResource.getUri(), newResourceDB);
		final Collection<ValueEntity> existingFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(existingResource.getUri(), existingResourceDB);
		// 3.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeExactGDMValueMatcher firstDegreeExactGDMValueMatcher = new FirstDegreeExactGDMValueMatcher(existingFlatResourceNodeValueEntities, newFlatResourceNodeValueEntities);
		final Collection<String> firstDegreeExactGDMValueMatches = firstDegreeExactGDMValueMatcher.getMatches();
		// TODO: utilise matched value entities for path marking in graph
		final Collection<ValueEntity> newFirstDegreeExactGDMValueMatches = firstDegreeExactGDMValueMatcher.getMatches(firstDegreeExactGDMValueMatcher.getNewValueEntities());
		final Collection<ValueEntity> existingFirstDegreeExactGDMValueMatches = firstDegreeExactGDMValueMatcher.getMatches(firstDegreeExactGDMValueMatcher.getExistingValueEntities());
		final Collection<ValueEntity> newFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher.getNonMatches(
				firstDegreeExactGDMValueMatcher.getNewValueEntities());
		final Collection<ValueEntity> existingFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher.getNonMatches(
				firstDegreeExactGDMValueMatcher.getExistingValueEntities());
		// 4. identify modifications of resource node-based statements or non-hierarchical sub graphs
		// 4.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeModificationGDMValueMatcher firstDegreeModificationGDMValueMatcher = new FirstDegreeModificationGDMValueMatcher(existingFirstDegreeExactGDMValueNonMatches, newFirstDegreeExactGDMValueNonMatches);
		final Map<ValueEntity, ValueEntity> firstDegreeModificationGDMValueModifications = firstDegreeModificationGDMValueMatcher.getModifications();
		// TODO: utilise matched value entities for path marking in graph
		final Collection<ValueEntity> newFirstDegreeModificationGDMValueMatches = firstDegreeModificationGDMValueMatcher.getMatches(firstDegreeModificationGDMValueMatcher.getNewValueEntities());
		final Collection<ValueEntity> existingFirstDegreeModificationGDMValueMatches = firstDegreeModificationGDMValueMatcher.getMatches(firstDegreeModificationGDMValueMatcher.getExistingValueEntities());
		// = resource node gdm value entity additions
		final Collection<ValueEntity> newFirstDegreeModificationGDMValueNonMatches = firstDegreeModificationGDMValueMatcher.getNonMatches(firstDegreeModificationGDMValueMatcher.getNewValueEntities());
		// = resource node gdm value entity removals
		final Collection<ValueEntity> existingFirstDegreeModificationGDMValueNonMatches = firstDegreeModificationGDMValueMatcher.getNonMatches(firstDegreeModificationGDMValueMatcher.getExistingValueEntities());
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
