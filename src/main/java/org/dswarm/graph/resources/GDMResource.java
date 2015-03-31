/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.resources;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.dswarm.common.DMPStatics;
import org.dswarm.common.model.AttributePath;
import org.dswarm.common.model.ContentSchema;
import org.dswarm.common.model.util.AttributePathUtil;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.Changeset;
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
import org.dswarm.graph.delta.util.ChangesetUtil;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.gdm.DataModelGDMNeo4jProcessor;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.gdm.SimpleGDMNeo4jProcessor;
import org.dswarm.graph.gdm.parse.DataModelGDMNeo4jHandler;
import org.dswarm.graph.gdm.parse.GDMChangesetParser;
import org.dswarm.graph.gdm.parse.GDMHandler;
import org.dswarm.graph.gdm.parse.GDMModelParser;
import org.dswarm.graph.gdm.parse.GDMNeo4jHandler;
import org.dswarm.graph.gdm.parse.GDMParser;
import org.dswarm.graph.gdm.parse.GDMResourceParser;
import org.dswarm.graph.gdm.parse.GDMUpdateHandler;
import org.dswarm.graph.gdm.parse.GDMUpdateParser;
import org.dswarm.graph.gdm.parse.Neo4jDeltaGDMHandler;
import org.dswarm.graph.gdm.parse.SimpleGDMNeo4jHandler;
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
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.parse.Neo4jUpdateHandler;
import org.dswarm.graph.versioning.VersioningStatics;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;

/**
 * @author tgaengler
 */
@Path("/gdm")
public class GDMResource {

	private static final Logger LOG = LoggerFactory.getLogger(GDMResource.class);

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper             objectMapper;
	private final TestGraphDatabaseFactory impermanentGraphDatabaseFactory;
	private static final String IMPERMANENT_GRAPH_DATABASE_PATH = "target/test-data/impermanent-db/";

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

	/**
	 * multipart/mixed payload contains two body parts:<br/>
	 * - first body part is the content (i.e. the real data)<br/>
	 * - second body part is the metadata (i.e. a JSON object with mandatory and obligatory properties for processing the
	 * content):<br/>
	 * - "data_model_URI" (mandatory) - "content_schema" (obligatory) - "deprecate_missing_records" (obligatory) -
	 * "record_class_uri" (mandatory for "deprecate_missing_records")
	 *
	 * @param multiPart
	 * @param database
	 * @return
	 * @throws DMPGraphException
	 * @throws IOException
	 */
	@POST
	@Path("/put")
	@Consumes("multipart/mixed")
	public Response writeGDM(final MultiPart multiPart, @Context final GraphDatabaseService database) throws DMPGraphException, IOException {

		LOG.debug("try to process GDM statements and write them into graph db");

		final List<BodyPart> bodyParts = getBodyParts(multiPart);
		final InputStream content = getContent(bodyParts);
		final ObjectNode metadata = getMetadata(bodyParts);

		final Optional<String> optionalDataModelURI = getMetadataPart(DMPStatics.DATA_MODEL_URI_IDENTIFIER, metadata, true);
		final String dataModelURI = optionalDataModelURI.get();

		// TODO: maybe do this later, when everything else is checked
		Model model = getModel(content);

		LOG.debug("deserialized GDM statements that were serialised as JSON");
		LOG.debug("try to write GDM statements into graph db");

		final GDMNeo4jProcessor processor = new DataModelGDMNeo4jProcessor(database, dataModelURI);

		try {

			final GDMNeo4jHandler handler = new DataModelGDMNeo4jHandler(processor);
			final Optional<ContentSchema> optionalContentSchema = getContentSchema(metadata);

			// = new resources model, since existing, modified resources were already written to the DB
			final Pair<Model, Set<String>> result = calculateDeltaForDataModel(model, optionalContentSchema, dataModelURI, database, handler);

			model = result.first();

			final Optional<Boolean> optionalDeprecateMissingRecords = getDeprecateMissingRecordsFlag(metadata);

			if (optionalDeprecateMissingRecords.isPresent() && Boolean.TRUE.equals(optionalDeprecateMissingRecords.get())) {

				final Optional<String> optionalRecordClassURI = getMetadataPart(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, metadata, false);

				if (!optionalRecordClassURI.isPresent()) {

					throw new DMPGraphException("could not deprecate missing records, because no record class uri is given");
				}

				// deprecate missing records in DB

				final Set<String> processedResources = result.other();

				deprecateMissingRecords(processedResources, optionalRecordClassURI.get(), dataModelURI,
						((Neo4jUpdateHandler) handler.getHandler()).getVersionHandler().getLatestVersion(), processor);
			}

			if (model.size() > 0) {

				// parse model only, when model contains some resources

				final GDMParser parser = new GDMModelParser(model);
				parser.setGDMHandler(handler);
				parser.parse();
			} else {

				GDMResource.LOG.debug("model contains no resources, i.e., nothing needs to be written to the DB");
			}

			final Long size = handler.getHandler().getCountedStatements();

			if (size > 0) {

				// update data model version only when some statements are written to the DB
				((Neo4jUpdateHandler) handler.getHandler()).getVersionHandler().updateLatestVersion();
			}

			handler.getHandler().closeTransaction();

			content.close();

			LOG.debug("finished writing " + size + " GDM statements into graph db for data model URI '" + dataModelURI + "'");

			return Response.ok().build();

		} catch (final Exception e) {

			processor.getProcessor().failTx();

			content.close();

			LOG.error("couldn't write GDM statements into graph db: " + e.getMessage(), e);

			throw e;
		}
	}

	@POST
	@Path("/put")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeGDM(final InputStream inputStream, @Context final GraphDatabaseService database) throws DMPGraphException, IOException {

		LOG.debug("try to process GDM statements and write them into graph db");

		if (inputStream == null) {

			final String message = "input stream for write to graph DB request is null";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final ObjectMapper mapper = Util.getJSONObjectMapper();

		Model model = null;
		try {
			model = mapper.readValue(inputStream, Model.class);
		} catch (IOException e) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message, e);
		}

		if (model == null) {

			final String message = "could not deserialise GDM JSON for write to graph DB request";

			GDMResource.LOG.error(message);

			inputStream.close();

			throw new DMPGraphException(message);
		}

		LOG.debug("deserialized GDM statements that were serialised as Turtle and N3");

		LOG.debug("try to write GDM statements into graph db");

		final GDMNeo4jProcessor processor = new SimpleGDMNeo4jProcessor(database);

		try {

			final GDMHandler handler = new SimpleGDMNeo4jHandler(processor);
			final GDMParser parser = new GDMModelParser(model);
			parser.setGDMHandler(handler);
			parser.parse();
			handler.getHandler().closeTransaction();

			inputStream.close();

			LOG.debug("finished writing " + handler.getHandler().getCountedStatements() + " GDM statements into graph db");
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			inputStream.close();

			LOG.error("couldn't write GDM statements into graph db: " + e.getMessage(), e);

			throw e;
		}

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

		final String recordClassUri = json.get(DMPStatics.RECORD_CLASS_URI_IDENTIFIER).asText();
		final String dataModelUri = json.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();
		final JsonNode versionNode = json.get(DMPStatics.VERSION_IDENTIFIER);
		final Integer version;

		if (versionNode != null) {

			version = versionNode.asInt();
		} else {

			version = null;
		}

		final JsonNode atMostNode = json.get(DMPStatics.AT_MOST_IDENTIFIER);

		final Optional<Integer> optionalAtMost;

		if (atMostNode != null) {

			optionalAtMost = Optional.fromNullable(atMostNode.asInt());
		} else {

			optionalAtMost = Optional.absent();
		}

		GDMResource.LOG.debug("try to read GDM statements for data model uri = '" + dataModelUri + "' and record class uri = '" + recordClassUri
				+ "' and version = '" + version + "' from graph db");

		final GDMModelReader gdmReader = new PropertyGraphGDMModelReader(recordClassUri, dataModelUri, version, optionalAtMost, database);
		final Model model = gdmReader.read();

		String result = null;
		try {
			result = objectMapper.writeValueAsString(model);
		} catch (final JsonProcessingException e) {

			throw new DMPGraphException("some problems occur, while processing the JSON from the GDM model", e);
		}

		GDMResource.LOG.debug("finished reading '" + model.size() + "' GDM statements ('" + gdmReader.countStatements()
				+ "' via GDM reader) for data model uri = '" + dataModelUri + "' and record class uri = '" + recordClassUri + "' and version = '"
				+ version + "' from graph db");

		return Response.ok().entity(result).build();
	}

	private Pair<Model, Set<String>> calculateDeltaForDataModel(final Model model, final Optional<ContentSchema> optionalContentSchema,
			final String dataModelURI,
			final GraphDatabaseService permanentDatabase, final GDMUpdateHandler handler) throws DMPGraphException {

		GDMResource.LOG.debug("start calculating delta for model");

		final Model newResourcesModel = new Model();
		final Set<String> processedResources = new HashSet<>();

		// calculate delta resource-wise
		for (Resource newResource : model.getResources()) {

			final String resourceURI = newResource.getUri();
			final String hash = UUID.randomUUID().toString();
			final GraphDatabaseService newResourceDB = loadResource(newResource, IMPERMANENT_GRAPH_DATABASE_PATH + hash + "2");

			final Resource existingResource;
			final GDMResourceReader gdmReader;

			if (optionalContentSchema.isPresent() && optionalContentSchema.get().getRecordIdentifierAttributePath() != null) {

				// determine legacy resource identifier via content schema
				final String recordIdentifier = GraphDBUtil.determineRecordIdentifier(newResourceDB,
						optionalContentSchema.get().getRecordIdentifierAttributePath(), newResource.getUri());

				// try to retrieve existing model via legacy record identifier
				gdmReader = new PropertyGraphGDMResourceByIDReader(recordIdentifier, optionalContentSchema.get().getRecordIdentifierAttributePath(),
						dataModelURI,
						permanentDatabase);
			} else {

				// try to retrieve existing model via resource uri
				gdmReader = new PropertyGraphGDMResourceByURIReader(resourceURI, dataModelURI, permanentDatabase);
			}

			existingResource = gdmReader.read();

			if (existingResource == null) {

				// take new resource model, since there was no match in the data model graph for this resource identifier
				newResourcesModel.addResource(newResource);

				newResourceDB.shutdown();

				// we don't need to calculate the delta, since everything is new
				continue;
			}

			processedResources.add(existingResource.getUri());

			// final Model newResourceModel = new Model();
			// newResourceModel.addResource(resource);

			final GraphDatabaseService existingResourceDB = loadResource(existingResource, IMPERMANENT_GRAPH_DATABASE_PATH + hash + "1");

			final Changeset changeset = calculateDeltaForResource(existingResource, existingResourceDB, newResource, newResourceDB,
					optionalContentSchema);

			if (!changeset.hasChanges()) {

				// process changeset only, if it provides changes

				GDMResource.LOG.debug("no changes detected for this resource");

				shutDownDeltaDBs(existingResourceDB, newResourceDB);

				continue;
			}

			// write modified resources resource-wise - instead of the whole model at once.
			final GDMUpdateParser parser = new GDMChangesetParser(changeset, existingResource, existingResourceDB, newResourceDB);
			parser.setGDMHandler(handler);
			parser.parse();

			shutDownDeltaDBs(existingResourceDB, newResourceDB);
		}

		GDMResource.LOG.debug("finished calculating delta for model and writing changes to graph DB");

		// return only model with new, non-existing resources
		return Pair.of(newResourcesModel, processedResources);
	}

	private Changeset calculateDeltaForResource(final Resource existingResource, final GraphDatabaseService existingResourceDB,
			final Resource newResource, final GraphDatabaseService newResourceDB, final Optional<ContentSchema> optionalContentSchema)
			throws DMPGraphException {

		enrichModel(existingResourceDB, existingResource.getUri());
		enrichModel(newResourceDB, newResource.getUri());

		// GraphDBUtil.printNodes(existingResourceDB);
		// GraphDBUtil.printRelationships(existingResourceDB);
		// GraphDBUtil.printPaths(existingResourceDB, existingResource.getUri());
		// GraphDBPrintUtil.printDeltaRelationships(existingResourceDB);
		// final URL resURL = Resources.getResource("versioning/lic_dmp_v2.csv");
		// final String resURLString = resURL.toString();
		// try {
		// final URL existingResURL = new URL(newResource.getUri());
		// final String path = existingResURL.getPath();
		// final String uuid = path.substring(path.lastIndexOf("/") + 1, path.length());
		// final String newResURLString = resURLString + "." + uuid + ".txt";
		// final URL newResURL = new URL(newResURLString);
		// GraphDBPrintUtil.writeDeltaRelationships(newResourceDB, newResURL);
		// } catch (MalformedURLException e) {
		// e.printStackTrace();
		// }

		// GraphDBUtil.printNodes(newResourceDB);
		// GraphDBUtil.printRelationships(newResourceDB);
		// GraphDBUtil.printPaths(newResourceDB, newResource.getUri());
		// GraphDBPrintUtil.printDeltaRelationships(newResourceDB);

		final Map<Long, Long> changesetModifications = new HashMap<>();

		final Optional<AttributePath> optionalCommonAttributePath;

		if (optionalContentSchema.isPresent()) {

			optionalCommonAttributePath = AttributePathUtil.determineCommonAttributePath(optionalContentSchema.get());
		} else {

			optionalCommonAttributePath = Optional.absent();
		}

		if (optionalCommonAttributePath.isPresent()) {

			// do specific processing with content schema knowledge

			final AttributePath commonAttributePath = optionalCommonAttributePath.get();

			final Collection<CSEntity> newCSEntities = GraphDBUtil.getCSEntities(newResourceDB, newResource.getUri(), commonAttributePath,
					optionalContentSchema.get());
			final Collection<CSEntity> existingCSEntities = GraphDBUtil.getCSEntities(existingResourceDB, existingResource.getUri(),
					commonAttributePath, optionalContentSchema.get());

			// do delta calculation on enriched GDM models in graph
			// note: we can also follow a different strategy, i.e., all most exact steps first and the reduce this level, i.e., do
			// for
			// each exact level all steps first and continue afterwards (?)
			// 1. identify exact matches for cs entities
			// 1.1 hash with key, value(s) + entity order + value(s) order => matches complete cs entities
			// keep attention to sub entities of CS entities -> note: this needs to be done as part of the the exact cs entity =>
			// see step 7
			// matching as well, i.e., we need to be able to calc a hash from sub entities of the cs entities
			final FirstDegreeExactCSEntityMatcher exactCSMatcher = new FirstDegreeExactCSEntityMatcher(Optional.fromNullable(existingCSEntities),
					Optional.fromNullable(newCSEntities), existingResourceDB, newResourceDB, existingResource.getUri(), newResource.getUri());
			exactCSMatcher.match();

			final Optional<? extends Collection<CSEntity>> newExactCSNonMatches = exactCSMatcher.getNewEntitiesNonMatches();
			final Optional<? extends Collection<CSEntity>> existingExactCSNonMatches = exactCSMatcher.getExistingEntitiesNonMatches();
			final Optional<? extends Collection<ValueEntity>> newFirstDegreeExactCSValueNonMatches = CSEntityUtil
					.getValueEntities(newExactCSNonMatches);
			final Optional<? extends Collection<ValueEntity>> existingFirstDegreeExactCSValueNonMatches = CSEntityUtil
					.getValueEntities(existingExactCSNonMatches);
			// 1.2 hash with key, value + entity order + value order => matches value entities
			final FirstDegreeExactCSValueMatcher firstDegreeExactCSValueMatcher = new FirstDegreeExactCSValueMatcher(
					existingFirstDegreeExactCSValueNonMatches, newFirstDegreeExactCSValueNonMatches, existingResourceDB, newResourceDB,
					existingResource.getUri(), newResource.getUri());
			firstDegreeExactCSValueMatcher.match();

			final Optional<? extends Collection<ValueEntity>> newExactCSValueNonMatches = firstDegreeExactCSValueMatcher.getNewEntitiesNonMatches();
			final Optional<? extends Collection<ValueEntity>> existingExactCSValueNonMatches = firstDegreeExactCSValueMatcher
					.getExistingEntitiesNonMatches();
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
					Optional.fromNullable(existingSubGraphEntities), Optional.fromNullable(newSubGraphEntities), existingResourceDB, newResourceDB,
					existingResource.getUri(), newResource.getUri());
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

			for (final Map.Entry<ValueEntity, ValueEntity> modificationEntry : modificationCSMatcher.getModifications().entrySet()) {

				changesetModifications.put(modificationEntry.getKey().getNodeId(), modificationEntry.getValue().getNodeId());
			}

			for (final Map.Entry<SubGraphLeafEntity, SubGraphLeafEntity> firstDegreeModificationSubGraphLeafEntityModificationEntry : firstDegreeModificationSubGraphLeafEntityMatcher
					.getModifications().entrySet()) {

				changesetModifications.put(firstDegreeModificationSubGraphLeafEntityModificationEntry.getKey().getNodeId(),
						firstDegreeModificationSubGraphLeafEntityModificationEntry.getValue().getNodeId());
			}
		}

		// 3. identify exact matches of resource node-based statements
		final Collection<ValueEntity> newFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(newResource.getUri(), newResourceDB);
		final Collection<ValueEntity> existingFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(existingResource.getUri(),
				existingResourceDB);
		// 3.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeExactGDMValueMatcher firstDegreeExactGDMValueMatcher = new FirstDegreeExactGDMValueMatcher(
				Optional.fromNullable(existingFlatResourceNodeValueEntities), Optional.fromNullable(newFlatResourceNodeValueEntities),
				existingResourceDB, newResourceDB, existingResource.getUri(), newResource.getUri());
		firstDegreeExactGDMValueMatcher.match();

		final Optional<? extends Collection<ValueEntity>> newFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher
				.getNewEntitiesNonMatches();
		final Optional<? extends Collection<ValueEntity>> existingFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher
				.getExistingEntitiesNonMatches();
		// 4. identify modifications of resource node-based statements
		// 4.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeModificationGDMValueMatcher firstDegreeModificationGDMValueMatcher = new FirstDegreeModificationGDMValueMatcher(
				existingFirstDegreeExactGDMValueNonMatches, newFirstDegreeExactGDMValueNonMatches, existingResourceDB, newResourceDB,
				existingResource.getUri(), newResource.getUri());
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

		if (!isExistingResourceMatchedCompletely) {

			throw new DMPGraphException("existing resource wasn't matched completely by the delta algo");
		}

		final boolean isNewResourceMatchedCompletely = GraphDBUtil.checkGraphMatchingCompleteness(newResourceDB);

		if (!isNewResourceMatchedCompletely) {

			throw new DMPGraphException("new resource wasn't matched completely by the delta algo");
		}

		// traverse resource graphs to extract changeset
		final PropertyGraphDeltaGDMSubGraphWorker addedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(newResource.getUri(),
				DeltaState.ADDITION, newResourceDB);
		final Map<String, Statement> addedStatements = addedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker removedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(
				existingResource.getUri(), DeltaState.DELETION, existingResourceDB);
		final Map<String, Statement> removedStatements = removedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker newModifiedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(newResource.getUri(),
				DeltaState.MODIFICATION, newResourceDB);
		final Map<String, Statement> newModifiedStatements = newModifiedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker existingModifiedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(
				existingResource.getUri(), DeltaState.MODIFICATION, existingResourceDB);
		final Map<String, Statement> existingModifiedStatements = existingModifiedStatementsPGDGDMSGWorker.work();

		for (final Map.Entry<ValueEntity, ValueEntity> firstDegreeModificationGDMValueModificationEntry : firstDegreeModificationGDMValueMatcher
				.getModifications().entrySet()) {

			changesetModifications.put(firstDegreeModificationGDMValueModificationEntry.getKey().getNodeId(),
					firstDegreeModificationGDMValueModificationEntry.getValue().getNodeId());
		}

		final Map<Long, Statement> preparedExistingModifiedStatements = ChangesetUtil.providedModifiedStatements(existingModifiedStatements);
		final Map<Long, Statement> preparedNewModifiedStatements = ChangesetUtil.providedModifiedStatements(newModifiedStatements);

		// return a changeset model (i.e. with information for add, delete, update per triple)
		return new Changeset(addedStatements, removedStatements, changesetModifications, preparedExistingModifiedStatements,
				preparedNewModifiedStatements);
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

	private void enrichModel(final GraphDatabaseService graphDB, final String resourceUri) throws DMPGraphException {

		final GDMWorker worker = new PropertyEnrichGDMWorker(resourceUri, graphDB);
		worker.work();
	}

	private void shutDownDeltaDBs(final GraphDatabaseService existingResourceDB, final GraphDatabaseService newResourceDB) {

		GDMResource.LOG.debug("start shutting down working graph data model DBs for resources");

		// should probably be delegated to a background worker thread, since it looks like that shutting down the working graph
		// DBs take some (for whatever reason)
		final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
		service.submit(new Callable<Void>() {

			public Void call() {

				newResourceDB.shutdown();
				existingResourceDB.shutdown();

				return null;
			}
		});

		GDMResource.LOG.debug("finished shutting down working graph data model DBs for resources");
	}

	private void deprecateMissingRecords(final Set<String> processedResources, final String recordClassUri, final String dataModelUri,
			final int latestVersion, final GDMNeo4jProcessor processor) throws DMPGraphException {

		// determine all record URIs of the data model
		// how? - via record class?

		processor.getProcessor().ensureRunningTx();

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassUri);

			final ResourceIterator<Node> recordNodes = processor.getProcessor().getDatabase()
					.findNodes(recordClassLabel, GraphStatics.DATA_MODEL_PROPERTY, dataModelUri);

			if (recordNodes == null) {

				GDMResource.LOG.debug("finished read data model record nodes TX successfully");

				return;
			}

			final Set<Node> notProcessedResources = new HashSet<>();

			while(recordNodes.hasNext()) {

				final Node recordNode = recordNodes.next();

				final String resourceUri = (String) recordNode.getProperty(GraphStatics.URI_PROPERTY, null);

				if (resourceUri == null) {

					LOG.debug("there is no resource URI at record node '" + recordNode.getId() + "'");

					continue;
				}

				if (!processedResources.contains(resourceUri)) {

					notProcessedResources.add(recordNode);

					// TODO: do also need to deprecate the record nodes themselves?
				}
			}

			for (final Node notProcessedResource : notProcessedResources) {

				final Iterable<org.neo4j.graphdb.Path> notProcessedResourcePaths = GraphDBUtil.getResourcePaths(processor.getProcessor()
						.getDatabase(), notProcessedResource);

				if (notProcessedResourcePaths == null) {

					continue;
				}

				for (final org.neo4j.graphdb.Path notProcessedResourcePath : notProcessedResourcePaths) {

					final Iterable<Relationship> rels = notProcessedResourcePath.relationships();

					if (rels == null) {

						continue;
					}

					for (final Relationship rel : rels) {

						rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, latestVersion);
					}
				}
			}

			recordNodes.close();
		} catch (final Exception e) {

			final String message = "couldn't determine record URIs of the data model successfully";

			processor.getProcessor().failTx();

			GDMResource.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	private List<BodyPart> getBodyParts(final MultiPart multiPart) throws DMPGraphException {

		if (multiPart == null) {

			final String message = "couldn't write GDM, no multipart payload available";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final List<BodyPart> bodyParts = multiPart.getBodyParts();

		if (bodyParts == null || bodyParts.isEmpty()) {

			final String message = "couldn't write GDM, no body parts available";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		if (bodyParts.size() < 2) {

			final String message = "couldn't write GDM, there must be a content and a metadata body part";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		return bodyParts;
	}

	private InputStream getContent(final List<BodyPart> bodyParts) throws DMPGraphException {

		final BodyPart contentBodyPart = bodyParts.get(0);

		if (contentBodyPart == null) {

			final String message = "couldn't write GDM, no content part available";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final BodyPartEntity bpe = (BodyPartEntity) contentBodyPart.getEntity();

		if (bpe == null) {

			final String message = "couldn't write GDM, no content part entity available";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final InputStream gdmInputStream = bpe.getInputStream();

		if (gdmInputStream == null) {

			final String message = "input stream for write to graph DB request is null";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		return gdmInputStream;
	}

	private ObjectNode getMetadata(final List<BodyPart> bodyParts) throws DMPGraphException {

		final BodyPart metadataBodyPart = bodyParts.get(1);

		if (metadataBodyPart == null) {

			final String message = "couldn't write GDM, no metadata part available";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final String metadataString = metadataBodyPart.getEntityAs(String.class);

		if (metadataString == null) {

			final String message = "couldn't write GDM, no metadata entity part available";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		try {

			return objectMapper.readValue(metadataString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = "couldn't write GDM, couldn't deserialize metadata part";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message, e);
		}
	}

	private Model getModel(final InputStream content) throws DMPGraphException, IOException {

		Model model;

		try {

			model = objectMapper.readValue(content, Model.class);
		} catch (IOException e) {

			final String message = "couldn't write GDM, could not deserialise GDM JSON";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message, e);
		}

		if (model == null) {

			final String message = "couldn't write GDM, deserialised GDM JSON is null";

			GDMResource.LOG.error(message);

			content.close();

			throw new DMPGraphException(message);
		}

		return model;
	}

	private Optional<JsonNode> getMetadataPartNode(final String property, final ObjectNode metadata, final boolean mandatory)
			throws DMPGraphException {

		final JsonNode metadataPartNode = metadata.get(property);

		if (metadataPartNode == null) {

			final String message;

			if (mandatory) {
				message = "couldn't write GDM, mandatory property '" + property + "' is not available in request metadata";

				GDMResource.LOG.error(message);

				throw new DMPGraphException(message);
			} else {

				message = "couldn't find obligatory property '" + property + "' in request metadata";

				GDMResource.LOG.debug(message);

				return Optional.absent();
			}
		}

		return Optional.of(metadataPartNode);
	}

	private Optional<String> getMetadataPart(final String property, final ObjectNode metadata, final boolean mandatory) throws DMPGraphException {

		final Optional<JsonNode> optionalMetadataPartNode = getMetadataPartNode(property, metadata, mandatory);

		if (!optionalMetadataPartNode.isPresent()) {

			return Optional.absent();
		}

		final JsonNode metadataPartNode = optionalMetadataPartNode.get();

		final String metadataPartValue = metadataPartNode.asText();

		if (metadataPartValue == null) {

			final String message;

			if (mandatory) {
				message = "couldn't write GDM, mandatory value for property '" + property + "' is not available in request metadata";

				GDMResource.LOG.error(message);

				throw new DMPGraphException(message);
			} else {

				message = "couldn't find obligatory value for property '" + property + "' in request metadata";

				GDMResource.LOG.debug(message);

				return Optional.absent();
			}
		}

		return Optional.of(metadataPartValue);
	}

	private Optional<ContentSchema> getContentSchema(final ObjectNode metadata) throws DMPGraphException {

		final Optional<JsonNode> optionalContentSchemaJSON = getMetadataPartNode(DMPStatics.CONTENT_SCHEMA_IDENTIFIER, metadata, false);

		if (!optionalContentSchemaJSON.isPresent()) {

			return Optional.absent();
		}

		try {

			final String contentSchemaJSONString = objectMapper.writeValueAsString(optionalContentSchemaJSON.get());
			final ContentSchema contentSchema = objectMapper.readValue(contentSchemaJSONString, ContentSchema.class);

			return Optional.fromNullable(contentSchema);
		} catch (final IOException e) {

			final String message = "could not deserialise content schema JSON for write from graph DB request";

			GDMResource.LOG.debug(message);

			return Optional.absent();
		}
	}

	private Optional<Boolean> getDeprecateMissingRecordsFlag(final ObjectNode metadata) throws DMPGraphException {

		final Optional<String> optionalDeprecateMissingRecords = getMetadataPart(DMPStatics.DEPRECATE_MISSING_RECORDS_IDENTIFIER, metadata, false);

		final Optional<Boolean> result;

		if (optionalDeprecateMissingRecords.isPresent()) {

			result = Optional.fromNullable(Boolean.valueOf(optionalDeprecateMissingRecords.get()));
		} else {

			result = Optional.of(Boolean.FALSE);
		}

		return result;
	}
}
