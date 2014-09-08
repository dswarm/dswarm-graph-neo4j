package org.dswarm.graph.resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
import org.dswarm.graph.delta.Changeset;
import org.dswarm.graph.delta.ContentSchema;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.FirstDegreeExactCSEntityMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactCSValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactGDMValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactSubGraphEntityMatcher;
import org.dswarm.graph.delta.match.FirstDegreeExactSubGraphLeafEntityMatcher;
import org.dswarm.graph.delta.match.FirstDegreeModificationCSValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeModificationGDMValueMatcher;
import org.dswarm.graph.delta.match.FirstDegreeModificationSubGraphLeafEntityMatcher;
import org.dswarm.graph.delta.match.ModificationMatcher;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.dswarm.graph.delta.match.model.util.CSEntityUtil;
import org.dswarm.graph.delta.util.AttributePathUtil;
import org.dswarm.graph.delta.util.ChangesetUtil;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.gdm.parse.GDMChangesetParser;
import org.dswarm.graph.gdm.parse.GDMHandler;
import org.dswarm.graph.gdm.parse.GDMModelParser;
import org.dswarm.graph.gdm.parse.GDMParser;
import org.dswarm.graph.gdm.parse.GDMResourceParser;
import org.dswarm.graph.gdm.parse.GDMUpdateHandler;
import org.dswarm.graph.gdm.parse.GDMUpdateParser;
import org.dswarm.graph.gdm.parse.Neo4jDeltaGDMHandler;
import org.dswarm.graph.gdm.parse.Neo4jGDMHandler;
import org.dswarm.graph.gdm.parse.Neo4jGDMWProvenanceHandler;
import org.dswarm.graph.gdm.parse.Neo4jGDMWProvenanceUpdateHandler;
import org.dswarm.graph.gdm.read.GDMModelReader;
import org.dswarm.graph.gdm.read.GDMResourceReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMModelReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMResourceByIDReader;
import org.dswarm.graph.gdm.read.PropertyGraphGDMResourceByURIReader;
import org.dswarm.graph.gdm.work.GDMWorker;
import org.dswarm.graph.gdm.work.PropertyEnrichGDMWorker;
import org.dswarm.graph.gdm.work.PropertyGraphDeltaGDMSubGraphWorker;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.json.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
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
	private static final String				IMPERMANENT_GRAPH_DATABASE_PATH	= "target/test-data/impermanent-db/";

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

			// = new resources model, since existing, modified resources were already written to the DB
			model = calculateDeltaForDataModel(model, contentSchema, resourceGraphURI, database);
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
		final JsonNode versionNode = json.get("version");
		final Integer version;

		if (versionNode != null) {

			version = versionNode.asInt();
		} else {

			version = null;
		}

		GDMResource.LOG.debug("try to read GDM statements for resource graph uri = '" + resourceGraphUri + "' and record class uri = '"
				+ recordClassUri + "' from graph db");

		final GDMModelReader gdmReader = new PropertyGraphGDMModelReader(recordClassUri, resourceGraphUri, version, database);
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
			final GraphDatabaseService permanentDatabase) throws DMPGraphException {

		GDMResource.LOG.debug("start calculating delta for model");

		final Model newResourcesModel = new Model();

		// calculate delta resource-wise
		for (Resource newResource : model.getResources()) {

			final String resourceURI = newResource.getUri();
			final String hash = UUID.randomUUID().toString();
			final GraphDatabaseService newResourceDB = loadResource(newResource, IMPERMANENT_GRAPH_DATABASE_PATH + hash + "2");

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

			if (existingResource == null) {

				// take new resource model, since there was no match in the provenance graph for this resource identifier
				newResourcesModel.addResource(newResource);

				newResourceDB.shutdown();

				// we don't need to calculate the delta, since everything is new
				continue;
			}

			// final Model newResourceModel = new Model();
			// newResourceModel.addResource(resource);

			final GraphDatabaseService existingResourceDB = loadResource(existingResource, IMPERMANENT_GRAPH_DATABASE_PATH + hash + "1");

			final Changeset changeset = calculateDeltaForResource(existingResource, existingResourceDB, newResource, newResourceDB, contentSchema);

			// TODO: we maybe should write modified resources resource-wise - instead of the whole model at once.
			final GDMUpdateHandler gdmHandler = new Neo4jGDMWProvenanceUpdateHandler(permanentDatabase, resourceGraphURI);
			final GDMUpdateParser parser = new GDMChangesetParser(changeset, existingResource, existingResourceDB, newResourceDB);
			parser.setGDMHandler(gdmHandler);
			parser.parse();
		}

		GDMResource.LOG.debug("finished calculating delta for model and writing changes to graph DB");

		// return only model with new, non-existing resources
		return newResourcesModel;
	}

	private Changeset calculateDeltaForResource(final Resource existingResource, final GraphDatabaseService existingResourceDB, final Resource newResource, final GraphDatabaseService newResourceDB,
			final ContentSchema contentSchema) throws DMPGraphException {

		enrichModel(existingResourceDB, existingResource.getUri());
		enrichModel(newResourceDB, newResource.getUri());

		// GraphDBUtil.printNodes(existingResourceDB);
		// GraphDBUtil.printRelationships(existingResourceDB);
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());
		// GraphDBPrintUtil.printDeltaRelationships(existingResourceDB);

		// GraphDBUtil.printNodes(newResourceDB);
		// GraphDBUtil.printRelationships(newResourceDB);
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());
		// GraphDBPrintUtil.printDeltaRelationships(newResourceDB);

		final Map<Long, Long> changesetModifications = new HashMap<>();

		final Optional<AttributePath> optionalCommonAttributePath = AttributePathUtil.determineCommonAttributePath(contentSchema);

		if(optionalCommonAttributePath.isPresent()) {

			// do specific processing with content schema knowledge

			final AttributePath commonAttributePath = optionalCommonAttributePath.get();

			final Collection<CSEntity> newCSEntities = GraphDBUtil
					.getCSEntities(newResourceDB, newResource.getUri(), commonAttributePath, contentSchema);
			final Collection<CSEntity> existingCSEntities = GraphDBUtil
					.getCSEntities(existingResourceDB, existingResource.getUri(), commonAttributePath,
							contentSchema);

			// do delta calculation on enriched GDM models in graph
			// note: we can also follow a different strategy, i.e., all most exact steps first and the reduce this level, i.e., do for
			// each exact level all steps first and continue afterwards (?)
			// 1. identify exact matches for cs entities
			// 1.1 hash with key, value(s) + entity order + value(s) order => matches complete cs entities
			// keep attention to sub entities of CS entities -> note: this needs to be done as part of the the exact cs entity => see step 7
			// matching as well, i.e., we need to be able to calc a hash from sub entities of the cs entities
			final FirstDegreeExactCSEntityMatcher exactCSMatcher = new FirstDegreeExactCSEntityMatcher(Optional.fromNullable(existingCSEntities), Optional.fromNullable(newCSEntities),
					existingResourceDB, newResourceDB, existingResource.getUri(), newResource.getUri());
			exactCSMatcher.match();

			final Optional<? extends Collection<CSEntity>> newExactCSNonMatches = exactCSMatcher.getNewEntitiesNonMatches();
			final Optional<? extends Collection<CSEntity>> existingExactCSNonMatches = exactCSMatcher.getExistingEntitiesNonMatches();
			final Optional<? extends Collection<ValueEntity>> newFirstDegreeExactCSValueNonMatches = CSEntityUtil.getValueEntities(newExactCSNonMatches);
			final Optional<? extends Collection<ValueEntity>> existingFirstDegreeExactCSValueNonMatches = CSEntityUtil.getValueEntities(existingExactCSNonMatches);
			// 1.2 hash with key, value + entity order + value order => matches value entities
			final FirstDegreeExactCSValueMatcher firstDegreeExactCSValueMatcher = new FirstDegreeExactCSValueMatcher(
					existingFirstDegreeExactCSValueNonMatches, newFirstDegreeExactCSValueNonMatches, existingResourceDB, newResourceDB,
					existingResource.getUri(), newResource.getUri());
			firstDegreeExactCSValueMatcher.match();

			final Optional<? extends Collection<ValueEntity>> newExactCSValueNonMatches = firstDegreeExactCSValueMatcher.getNewEntitiesNonMatches();
			final Optional<? extends Collection<ValueEntity>> existingExactCSValueNonMatches = firstDegreeExactCSValueMatcher.getExistingEntitiesNonMatches();
			// 1.3 hash with key, value + entity order => matches value entities
			// 1.4 hash with key, value => matches value entities
			// 2. identify modifications for cs entities
			// 2.1 hash with key + entity order + value order => matches value entities
			final ModificationMatcher<ValueEntity> modificationCSMatcher = new FirstDegreeModificationCSValueMatcher(existingExactCSValueNonMatches,
					newExactCSValueNonMatches, existingResourceDB, newResourceDB, existingResource.getUri(), newResource.getUri());
			modificationCSMatcher.match();

			// 2.2 hash with key + entity order => matches value entities
			// 2.3 hash with key => matches value entities

			// 7. identify non-matched CS entity sub graphs
			// TODO: remove this later
			GDMResource.LOG.debug("determine non-matched cs entity sub graphs for new cs entities");
			final Collection<SubGraphEntity> newSubGraphEntities = GraphDBUtil.determineNonMatchedCSEntitySubGraphs(newCSEntities, newResourceDB);
			// TODO: remove this later
			GDMResource.LOG.debug("determine non-matched cs entity sub graphs for existing entities");
			final Collection<SubGraphEntity> existingSubGraphEntities = GraphDBUtil.determineNonMatchedCSEntitySubGraphs(existingCSEntities,
					existingResourceDB);
			// 7.1 identify exact matches of (non-hierarchical) CS entity sub graphs
			// 7.1.1 key + predicate + sub graph hash + order
			final FirstDegreeExactSubGraphEntityMatcher firstDegreeExactSubGraphEntityMatcher = new FirstDegreeExactSubGraphEntityMatcher(
					Optional.fromNullable(existingSubGraphEntities), Optional.fromNullable(newSubGraphEntities), existingResourceDB, newResourceDB, existingResource.getUri(),
					newResource.getUri());
			firstDegreeExactSubGraphEntityMatcher.match();

			final Optional<? extends Collection<SubGraphEntity>> newFirstDegreeExactSubGraphEntityNonMatches = firstDegreeExactSubGraphEntityMatcher
					.getNewEntitiesNonMatches();
			final Optional<? extends Collection<SubGraphEntity>> existingFirstDegreeExactSubGraphEntityNonMatches = firstDegreeExactSubGraphEntityMatcher
					.getExistingEntitiesNonMatches();

			// 7.2 identify of partial matches (paths) of (non-hierarchical) CS entity sub graphs

			final Optional<? extends Collection<SubGraphLeafEntity>> newSubGraphLeafEntities = GraphDBUtil.getSubGraphLeafEntities(
					newFirstDegreeExactSubGraphEntityNonMatches, newResourceDB);
			final Optional<? extends Collection<SubGraphLeafEntity>> existingSubGraphLeafEntities = GraphDBUtil.getSubGraphLeafEntities(
					existingFirstDegreeExactSubGraphEntityNonMatches, existingResourceDB);
			// 7.2.1 key + predicate + sub graph leaf path hash + order
			final FirstDegreeExactSubGraphLeafEntityMatcher firstDegreeExactSubGraphLeafEntityMatcher = new FirstDegreeExactSubGraphLeafEntityMatcher(
					existingSubGraphLeafEntities, newSubGraphLeafEntities, existingResourceDB, newResourceDB, existingResource.getUri(),
					newResource.getUri());
			firstDegreeExactSubGraphLeafEntityMatcher.match();

			final Optional<? extends Collection<SubGraphLeafEntity>> newFirstDegreeExactSubGraphLeafEntityNonMatches = firstDegreeExactSubGraphLeafEntityMatcher
					.getNewEntitiesNonMatches();
			final Optional<? extends Collection<SubGraphLeafEntity>> existingFirstDegreeExactSubGraphLeafEntityNonMatches = firstDegreeExactSubGraphLeafEntityMatcher
					.getExistingEntitiesNonMatches();
			// 7.3 identify modifications of (non-hierarchical) sub graphs
			final FirstDegreeModificationSubGraphLeafEntityMatcher firstDegreeModificationSubGraphLeafEntityMatcher = new FirstDegreeModificationSubGraphLeafEntityMatcher(
					existingFirstDegreeExactSubGraphLeafEntityNonMatches, newFirstDegreeExactSubGraphLeafEntityNonMatches, existingResourceDB,
					newResourceDB, existingResource.getUri(), newResource.getUri());
			firstDegreeModificationSubGraphLeafEntityMatcher.match();

			for(final Map.Entry<ValueEntity, ValueEntity> modificationEntry : modificationCSMatcher.getModifications().entrySet()) {

				changesetModifications.put(modificationEntry.getKey().getNodeId(), modificationEntry.getValue().getNodeId());
			}

			for(final Map.Entry<SubGraphLeafEntity, SubGraphLeafEntity> firstDegreeModificationSubGraphLeafEntityModificationEntry : firstDegreeModificationSubGraphLeafEntityMatcher.getModifications().entrySet()) {

				changesetModifications.put(firstDegreeModificationSubGraphLeafEntityModificationEntry.getKey().getNodeId(), firstDegreeModificationSubGraphLeafEntityModificationEntry.getValue().getNodeId());
			}
		}

		// 3. identify exact matches of resource node-based statements
		final Collection<ValueEntity> newFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(newResource.getUri(), newResourceDB);
		final Collection<ValueEntity> existingFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(existingResource.getUri(),
				existingResourceDB);
		// 3.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeExactGDMValueMatcher firstDegreeExactGDMValueMatcher = new FirstDegreeExactGDMValueMatcher(
				Optional.fromNullable(existingFlatResourceNodeValueEntities), Optional.fromNullable(newFlatResourceNodeValueEntities), existingResourceDB, newResourceDB, existingResource.getUri(), newResource.getUri());
		firstDegreeExactGDMValueMatcher.match();

		final Optional<? extends Collection<ValueEntity>> newFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher
				.getNewEntitiesNonMatches();
		final Optional<? extends Collection<ValueEntity>> existingFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher
				.getExistingEntitiesNonMatches();
		// 4. identify modifications of resource node-based statements
		// 4.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeModificationGDMValueMatcher firstDegreeModificationGDMValueMatcher = new FirstDegreeModificationGDMValueMatcher(
				existingFirstDegreeExactGDMValueNonMatches, newFirstDegreeExactGDMValueNonMatches, existingResourceDB, newResourceDB, existingResource.getUri(), newResource.getUri());
		firstDegreeModificationGDMValueMatcher.match();

		// 5. identify additions in new model graph
		// => see above
		// 6. identify removals in existing model graph
		// => see above

		// TODO: do sub graph matching for node-based statements (?)

		//
		// note: mark matches or modifications after every step
		// maybe utilise confidence value for different matching approaches

		// check graph matching completeness
		final boolean isExistingResourceMatchedCompletely = GraphDBUtil.checkGraphMatchingCompleteness(existingResourceDB);

		if(!isExistingResourceMatchedCompletely) {

			throw new DMPGraphException("existing resource wasn't matched completely by the delta algo");
		}

		final boolean isNewResourceMatchedCompletely = GraphDBUtil.checkGraphMatchingCompleteness(newResourceDB);

		if(!isNewResourceMatchedCompletely) {

			throw new DMPGraphException("new resource wasn't matched completely by the delta algo");
		}

		// traverse resource graphs to extract changeset
		final PropertyGraphDeltaGDMSubGraphWorker addedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(newResource.getUri(), DeltaState.ADDITION, newResourceDB);
		final Map<String, Statement> addedStatements = addedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker removedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(existingResource.getUri(), DeltaState.DELETION, existingResourceDB);
		final Map<String, Statement> removedStatements = removedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker newModifiedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(newResource.getUri(), DeltaState.MODIFICATION, newResourceDB);
		final Map<String, Statement> newModifiedStatements = newModifiedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker existingModifiedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(existingResource.getUri(), DeltaState.MODIFICATION, existingResourceDB);
		final Map<String, Statement> existingModifiedStatements = existingModifiedStatementsPGDGDMSGWorker.work();

		for(final Map.Entry<ValueEntity, ValueEntity> firstDegreeModificationGDMValueModificationEntry : firstDegreeModificationGDMValueMatcher.getModifications().entrySet()) {

			changesetModifications.put(firstDegreeModificationGDMValueModificationEntry.getKey().getNodeId(), firstDegreeModificationGDMValueModificationEntry.getValue().getNodeId());
		}

		final Map<Long, Statement> preparedExistingModifiedStatements = ChangesetUtil.providedModifiedStatements(existingModifiedStatements);
		final Map<Long, Statement> preparedNewModifiedStatements = ChangesetUtil.providedModifiedStatements(newModifiedStatements);

		// return a changeset model (i.e. with information for add, delete, update per triple)
		return new Changeset(addedStatements, removedStatements, changesetModifications, preparedExistingModifiedStatements, preparedNewModifiedStatements);
	}

	private GraphDatabaseService loadResource(final Resource resource, final String impermanentGraphDatabaseDir) throws DMPGraphException {

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
