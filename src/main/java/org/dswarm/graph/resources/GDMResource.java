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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

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
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Func1;

import org.dswarm.common.DMPStatics;
import org.dswarm.common.model.Attribute;
import org.dswarm.common.model.AttributePath;
import org.dswarm.common.model.ContentSchema;
import org.dswarm.common.model.util.AttributePathUtil;
import org.dswarm.common.types.Tuple;
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
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.index.SchemaIndexUtils;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.json.stream.ModelBuilder;
import org.dswarm.graph.json.stream.ModelParser;
import org.dswarm.graph.json.util.Util;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.parse.Neo4jUpdateHandler;
import org.dswarm.graph.tx.Neo4jTransactionHandler;
import org.dswarm.graph.tx.TransactionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
@Path("/gdm")
public class GDMResource {

	private static final Logger LOG = LoggerFactory.getLogger(GDMResource.class);

	public static final int METADATA_BODY_PART = 0;
	public static final int CONTENT_BODY_PART  = 1;

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper             objectMapper;
	private final TestGraphDatabaseFactory impermanentGraphDatabaseFactory;
	private static final String IMPERMANENT_GRAPH_DATABASE_PATH = "target/test-data/impermanent-db/";

	private static final String READ_GDM_MODEL_TYPE     = "read GDM record from graph DB request";
	private static final String READ_GDM_RECORD_TYPE    = "read GDM record from graph DB request";
	private static final String SEARCH_GDM_RECORDS_TYPE = "search GDM records";

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
	 * - first body part is the metadata (i.e. a JSON object with mandatory and obligatory properties for processing the
	 * content):<br/>
	 *     - "data_model_URI" (mandatory)<br/>
	 *     - "content_schema" (obligatory)<br/>
	 *     - "deprecate_missing_records" (obligatory)<br/>
	 *     - "record_class_uri" (mandatory for "deprecate_missing_records")<br/>
	 * - second body part is the content (i.e. the real data)
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
	public Response writeGDM(final MultiPart multiPart, @Context final GraphDatabaseService database, @Context final HttpHeaders requestHeaders)
			throws DMPGraphException, IOException {

		LOG.debug("try to process GDM statements and write them into graph db");

		final String headers = readHeaders(requestHeaders);

		GDMResource.LOG.debug("try to process GDM statements and write them into graph db with\n{}", headers);

		final List<BodyPart> bodyParts = getBodyParts(multiPart);
		final ObjectNode metadata = getMetadata(bodyParts);
		final InputStream content = getContent(bodyParts);

		final Optional<String> optionalDataModelURI = getMetadataPart(DMPStatics.DATA_MODEL_URI_IDENTIFIER, metadata, true);
		final String dataModelURI = optionalDataModelURI.get();
		final Optional<Boolean> optionalEnableVersioning = getEnableVersioningFlag(metadata);

		final boolean enableVersioning;

		if (optionalEnableVersioning.isPresent()) {

			enableVersioning = optionalEnableVersioning.get();
		} else {

			enableVersioning = true;
		}

		final Tuple<Observable<Resource>, BufferedInputStream> modelTuple = getModel(content);
		final Observable<Resource> model = modelTuple.v1();
		final BufferedInputStream bis = modelTuple.v2();

		LOG.debug("deserialized GDM statements that were serialised as JSON");
		LOG.debug("try to write GDM statements into graph db");

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		final String prefixedDataModelURI = namespaceIndex.createPrefixedURI(dataModelURI);

		final GDMNeo4jProcessor processor = new DataModelGDMNeo4jProcessor(database, tx, namespaceIndex, prefixedDataModelURI);

		LOG.info("process GDM statements and write them into graph db for data model '{}' ('{}')", dataModelURI, prefixedDataModelURI);

		try {

			final GDMNeo4jHandler handler = new DataModelGDMNeo4jHandler(processor, enableVersioning);
			final Observable<Resource> newModel;
			final Observable<Void> deprecateRecordsObservable;

			// note: versioning is enable by default
			if (enableVersioning) {

				LOG.info("do versioning with GDM statements for data model '{}' ('{}')", dataModelURI, prefixedDataModelURI);

				final Optional<ContentSchema> optionalPrefixedContentSchema = getPrefixedContentSchema(metadata, namespaceIndex);

				// = new resources model, since existing, modified resources were already written to the DB
				final Tuple<Observable<Resource>, Observable<Long>> result = calculateDeltaForDataModel(model, optionalPrefixedContentSchema,
						prefixedDataModelURI,
						database,
						handler, namespaceIndex);
				final Observable<Resource> deltaModel = result.v1();

				final Optional<Boolean> optionalDeprecateMissingRecords = getDeprecateMissingRecordsFlag(metadata);

				if (optionalDeprecateMissingRecords.isPresent() && optionalDeprecateMissingRecords.get()) {

					final Optional<String> optionalRecordClassURI = getMetadataPart(DMPStatics.RECORD_CLASS_URI_IDENTIFIER, metadata, false);

					if (!optionalRecordClassURI.isPresent()) {

						throw new DMPGraphException("could not deprecate missing records, because no record class uri is given");
					}

					// deprecate missing records in DB

					final Observable<Long> processedResources = result.v2();

					deprecateRecordsObservable = deprecateMissingRecords(processedResources, optionalRecordClassURI.get(), dataModelURI,
							((Neo4jUpdateHandler) handler.getHandler())
									.getVersionHandler().getLatestVersion(), processor);
				} else {

					deprecateRecordsObservable = Observable.empty();
				}

				newModel = deltaModel;

				LOG.info("finished versioning with GDM statements for data model '{}' ('{}')", dataModelURI, prefixedDataModelURI);
			} else {

				newModel = model;
				deprecateRecordsObservable = Observable.empty();
			}

			//if (deltaModel.size() > 0) {

			// parse model only, when model contains some resources

			final GDMParser parser = new GDMModelParser(newModel);
			parser.setGDMHandler(handler);
			final Observable<Void> newResourcesObservable = parser.parse();
			//} else {

			//GDMResource.LOG.debug("model contains no resources, i.e., nothing needs to be written to the DB");
			//}

			try {

				final Iterator<Void> iterator = deprecateRecordsObservable.concatWith(newResourcesObservable).toBlocking().getIterator();

				if (!iterator.hasNext()) {

					LOG.debug("model contains no resources, i.e., nothing needs to be written to the DB");
				}

				while (iterator.hasNext()) {

					iterator.next();
				}
			} catch (final RuntimeException e) {

				throw new DMPGraphException(e.getMessage(), e.getCause());
			}

			final Long size = handler.getHandler().getCountedStatements();

			if (enableVersioning && size > 0) {

				// update data model version only when some statements are written to the DB
				((Neo4jUpdateHandler) handler.getHandler()).getVersionHandler().updateLatestVersion();
			}

			handler.getHandler().closeTransaction();

			bis.close();
			content.close();

			LOG.info(
					"finished writing {} resources with {} GDM statements (added {} relationships, added {} nodes (resources + bnodes + literals), added {} literals) into graph db for data model URI '{}' ('{}')",
					parser.parsedResources(), handler.getHandler().getCountedStatements(),
					handler.getHandler().getRelationshipsAdded(), handler.getHandler().getNodesAdded(), handler.getHandler().getCountedLiterals(),
					dataModelURI, prefixedDataModelURI);

			return Response.ok().build();

		} catch (final Exception e) {

			processor.getProcessor().failTx();

			bis.close();
			content.close();

			LOG.error("couldn't write GDM statements into graph db: {}", e.getMessage(), e);

			throw e;
		}
	}

	@POST
	@Path("/put")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeGDM(final InputStream inputStream, @Context final GraphDatabaseService database, @Context final HttpHeaders requestHeaders)
			throws DMPGraphException, IOException {

		LOG.debug("try to process GDM statements and write them into graph db");

		final String headers = readHeaders(requestHeaders);

		GDMResource.LOG.debug("try to process GDM statements and write them into graph db with\n{}", headers);

		if (inputStream == null) {

			final String message = "input stream for write to graph DB request is null";

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final BufferedInputStream bis = new BufferedInputStream(inputStream, 1024);

		final ModelParser modelParser = new ModelParser(bis);
		final Observable<Resource> model = modelParser.parse();

		LOG.debug("try to write GDM statements into graph db");

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);
		final GDMNeo4jProcessor processor = new SimpleGDMNeo4jProcessor(database, tx, namespaceIndex);

		try {

			final GDMHandler handler = new SimpleGDMNeo4jHandler(processor, true);
			final GDMParser parser = new GDMModelParser(model);
			parser.setGDMHandler(handler);
			final Observable<Void> processedResources = parser.parse();

			try {

				final Iterator<Void> iterator = processedResources.toBlocking().getIterator();

				if (!iterator.hasNext()) {

					LOG.debug("model contains no resources, i.e., nothing needs to be written to the DB");
				}

				while (iterator.hasNext()) {

					iterator.next();
				}
			} catch (final RuntimeException e) {

				throw new DMPGraphException(e.getMessage(), e.getCause());
			}

			handler.getHandler().closeTransaction();

			bis.close();
			inputStream.close();

			LOG.debug(
					"finished writing {} resources with {} GDM statements (added {} relationships, added {} nodes (resources + bnodes + literals), added {} literals) into graph db",
					parser.parsedResources(), handler.getHandler().getCountedStatements(),
					handler.getHandler().getRelationshipsAdded(), handler.getHandler().getNodesAdded(), handler.getHandler().getCountedLiterals());
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			bis.close();
			inputStream.close();

			LOG.error("couldn't write GDM statements into graph db: {}", e.getMessage(), e);

			throw e;
		}

		return Response.ok().build();
	}

	@POST
	@Path("/get")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response readGDM(final String jsonObjectString, @Context final GraphDatabaseService database, @Context final HttpHeaders requestHeaders)
			throws DMPGraphException {

		final String headers = readHeaders(requestHeaders);

		GDMResource.LOG.debug("try to read GDM statements from graph db with\n{}", headers);

		final ObjectNode requestJSON = deserializeJSON(jsonObjectString, READ_GDM_MODEL_TYPE);

		final String recordClassUri = requestJSON.get(DMPStatics.RECORD_CLASS_URI_IDENTIFIER).asText();
		final String dataModelUri = requestJSON.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();
		final Optional<Integer> optionalVersion = getIntValue(DMPStatics.VERSION_IDENTIFIER, requestJSON);
		final Optional<Integer> optionalAtMost = getIntValue(DMPStatics.AT_MOST_IDENTIFIER, requestJSON);

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);
		final String prefixedRecordClassURI = namespaceIndex.createPrefixedURI(recordClassUri);
		final String prefixedDataModelURI = namespaceIndex.createPrefixedURI(dataModelUri);

		GDMResource.LOG
				.info("try to read GDM statements for data model uri = '{}' ('{}') and record class uri = '{}' ('{}') and version = '{}' from graph db",
						dataModelUri, prefixedDataModelURI, recordClassUri, prefixedRecordClassURI, optionalVersion);

		final GDMModelReader gdmReader = new PropertyGraphGDMModelReader(prefixedRecordClassURI, prefixedDataModelURI, optionalVersion,
				optionalAtMost, database, tx, namespaceIndex);

		final StreamingOutput stream = new StreamingOutput() {

			@Override
			public void write(final OutputStream os) throws IOException, WebApplicationException {

				try {

					final BufferedOutputStream bos = new BufferedOutputStream(os, 1024);
					final Optional<ModelBuilder> optionalModelBuilder = gdmReader.read(bos);

					if (optionalModelBuilder.isPresent()) {

						final ModelBuilder modelBuilder = optionalModelBuilder.get();
						modelBuilder.build();
						bos.flush();
						os.flush();
						bos.close();
						os.close();

						GDMResource.LOG
								.info("finished reading '{}' resources with '{}' GDM statements ('{}' via GDM reader) for data model uri = '{}' ('{}') and record class uri = '{}' ('{}') and version = '{}' from graph db",
										gdmReader.readResources(), gdmReader.countStatements(), gdmReader.countStatements(), dataModelUri,
										prefixedDataModelURI,
										recordClassUri, prefixedRecordClassURI, optionalVersion);
					} else {

						bos.close();
						os.close();

						GDMResource.LOG
								.info("couldn't find any GDM statements for data model uri = '{}' ('{}') and record class uri = '{}' ('{}') and version = '{}' from graph db",
										dataModelUri, prefixedDataModelURI, recordClassUri, prefixedRecordClassURI, optionalVersion);
					}
				} catch (final DMPGraphException e) {

					throw new WebApplicationException(e);
				}
			}
		};

		return Response.ok(stream, MediaType.APPLICATION_JSON_TYPE).build();
	}

	@POST
	@Path("/getrecord")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response readGDMRecord(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		GDMResource.LOG.debug("try to read GDM record from graph db");

		final ObjectNode requestJSON = deserializeJSON(jsonObjectString, READ_GDM_RECORD_TYPE);

		final Optional<String> optionalRecordUri = getStringValue(DMPStatics.RECORD_URI_IDENTIFIER, requestJSON);
		final String dataModelUri = requestJSON.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();
		final Optional<Integer> optionalVersion = getIntValue(DMPStatics.VERSION_IDENTIFIER, requestJSON);
		final Optional<String> optionalLegacyRecordIdentifierAP = getStringValue(DMPStatics.LEGACY_RECORD_IDENTIFIER_ATTRIBUTE_PATH, requestJSON);
		final Optional<String> optionalRecordId = getStringValue(DMPStatics.RECORD_ID_IDENTIFIER, requestJSON);

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		final String prefixedDataModelURI = namespaceIndex.createPrefixedURI(dataModelUri);

		final GDMResourceReader gdmReader;
		final String requestParameter;

		if (optionalRecordUri.isPresent()) {

			final String recordURI = optionalRecordUri.get();
			final String prefixedRecordURI = namespaceIndex.createPrefixedURI(recordURI);

			requestParameter = String.format("and record uri = '%s' ('%s')", recordURI, prefixedRecordURI);

			GDMResource.LOG
					.debug("try to read GDM record for data model uri = '{}' ('{}') {} and version = '{}' from graph db",
							dataModelUri, prefixedDataModelURI, requestParameter, optionalVersion);

			gdmReader = new PropertyGraphGDMResourceByURIReader(prefixedRecordURI, prefixedDataModelURI, optionalVersion, database, tx,
					namespaceIndex);
		} else if (optionalLegacyRecordIdentifierAP.isPresent() && optionalRecordId.isPresent()) {

			final AttributePath legacyRecordIdentifierAP = AttributePathUtil.parseAttributePathString(optionalLegacyRecordIdentifierAP.get());
			final AttributePath prefixedLegacyRecordIdentifierAP = prefixAttributePath(legacyRecordIdentifierAP, namespaceIndex);
			final String recordId = optionalRecordId.get();

			requestParameter = String
					.format("and legacy record identifier attribute path = '%s' ('%s') and record identifier = '%s'", legacyRecordIdentifierAP,
							prefixedLegacyRecordIdentifierAP, recordId);

			GDMResource.LOG
					.info("try to read GDM record for data model uri = '{}' ('{}') {} and version = '{}' from graph db",
							dataModelUri, prefixedDataModelURI, requestParameter, optionalVersion);

			gdmReader = new PropertyGraphGDMResourceByIDReader(recordId, prefixedLegacyRecordIdentifierAP, prefixedDataModelURI, optionalVersion,
					database, tx, namespaceIndex);
		} else {

			throw new DMPGraphException(
					"no identifiers given to retrieve a GDM record from the graph db. Please specify a record URI or legacy record identifier attribute path + record identifier");
		}

		final Resource resource = gdmReader.read();

		if (resource == null) {

			GDMResource.LOG.info(
					"no record found for data mode uri = '{}' ('{}') {} and version = '{}' from graph db",
					dataModelUri, prefixedDataModelURI, requestParameter, optionalVersion);

			return Response.status(404).build();
		}

		final String result = serializeJSON(resource, READ_GDM_RECORD_TYPE);

		GDMResource.LOG
				.info("finished reading '{}' GDM statements ('{}' via GDM reader) for data model uri = '{}' ('{}') {} and version = '{}' from graph db",
						resource.size(), gdmReader.countStatements(), dataModelUri, prefixedDataModelURI, requestParameter, optionalVersion);

		return Response.ok().entity(result).build();
	}

	@POST
	@Path("/searchrecords")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response searchGDMRecords(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		GDMResource.LOG.debug("try to {} in graph db", SEARCH_GDM_RECORDS_TYPE);

		final ObjectNode requestJSON = deserializeJSON(jsonObjectString, READ_GDM_RECORD_TYPE);

		final String keyAPString = requestJSON.get(DMPStatics.KEY_ATTRIBUTE_PATH_IDENTIFIER).asText();
		final String searchValue = requestJSON.get(DMPStatics.SEARCH_VALUE_IDENTIFIER).asText();
		final String dataModelUri = requestJSON.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();
		final Optional<Integer> optionalVersion = getIntValue(DMPStatics.VERSION_IDENTIFIER, requestJSON);

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		final String prefixedDataModelURI = namespaceIndex.createPrefixedURI(dataModelUri);

		final AttributePath keyAP = AttributePathUtil.parseAttributePathString(keyAPString);
		final AttributePath prefixedKeyAP = prefixAttributePath(keyAP, namespaceIndex);

		GDMResource.LOG
				.info("try to search GDM records for key attribute path = '{}' ('{}') and search value = '{}' in data model '{}' ('{}') with version = '{}' from graph db",
						keyAP, prefixedKeyAP, searchValue, dataModelUri, prefixedDataModelURI, optionalVersion);

		final Collection<String> recordURIs = GraphDBUtil.determineRecordUris(searchValue, prefixedKeyAP, prefixedDataModelURI, database);

		if (recordURIs == null || recordURIs.isEmpty()) {

			GDMResource.LOG
					.info("couldn't find any record for key attribute path = '{}' ('{}') and search value = '{}' in data model '{}' ('{}') with version = '{}' from graph db",
							keyAP, prefixedKeyAP, searchValue, dataModelUri, prefixedDataModelURI, optionalVersion);

			final StreamingOutput stream = new StreamingOutput() {

				@Override
				public void write(final OutputStream os) throws IOException, WebApplicationException {

					final BufferedOutputStream bos = new BufferedOutputStream(os, 1024);
					final ModelBuilder modelBuilder = new ModelBuilder(bos);

					modelBuilder.build();
					bos.flush();
					os.flush();
					bos.close();
					os.close();
				}
			};

			return Response.ok(stream, MediaType.APPLICATION_JSON_TYPE).build();
		}

		final StreamingOutput stream = new StreamingOutput() {

			@Override
			public void write(final OutputStream os) throws IOException, WebApplicationException {

				try {

					final BufferedOutputStream bos = new BufferedOutputStream(os, 1024);
					final ModelBuilder modelBuilder = new ModelBuilder(bos);

					int resourcesSize = 0;
					long statementsSize = 0;

					for (final String recordUri : recordURIs) {

						final GDMResourceReader gdmReader = new PropertyGraphGDMResourceByURIReader(recordUri, prefixedDataModelURI,
								optionalVersion, database, tx, namespaceIndex);

						try {

							final Resource resource = gdmReader.read();

							modelBuilder.addResource(resource);

							resourcesSize++;
							statementsSize += resource.size();
						} catch (final DMPGraphException e) {

							GDMResource.LOG.debug("couldn't retrieve record for record URI '{}'", recordUri);
						}
					}

					modelBuilder.build();
					bos.flush();
					os.flush();
					bos.close();
					os.close();

					if (resourcesSize > 0) {

						GDMResource.LOG
								.info("finished reading '{} records with '{}' GDM statements for key attribute path = '{}' ('{}') and search value = '{}' in data model '{}' ('{}') with version = '{}' from graph db",
										resourcesSize, statementsSize, keyAP, prefixedKeyAP, searchValue, dataModelUri, prefixedDataModelURI,
										optionalVersion);
					} else {

						GDMResource.LOG
								.info("couldn't retrieve any record for key attribute path = '{}' ('{}') and search value = '{}' in data model '{}' ('{}') with version = '{}' from graph db",
										keyAP, prefixedKeyAP, searchValue, dataModelUri, prefixedDataModelURI, optionalVersion);
					}
				} catch (final DMPGraphException e) {

					throw new WebApplicationException(e);
				}
			}
		};

		return Response.ok(stream, MediaType.APPLICATION_JSON_TYPE).build();
	}

	private Tuple<Observable<Resource>, Observable<Long>> calculateDeltaForDataModel(
			final Observable<Resource> model,
			final Optional<ContentSchema> optionalPrefixedContentSchema,
			final String prefixedDataModelURI,
			final GraphDatabaseService permanentDatabase,
			final GDMUpdateHandler handler,
			final NamespaceIndex namespaceIndex) throws DMPGraphException {

		GDMResource.LOG.debug("start calculating delta for model");

		final Set<Long> processedResources = new HashSet<>();

		// calculate delta resource-wise
		final Observable<Resource> newResources = model.flatMap(new Func1<Resource, Observable<Resource>>() {

			@Override public Observable<Resource> call(final Resource newResource) {

				try {

					final String resourceURI = newResource.getUri();
					final String prefixedResourceURI = namespaceIndex.createPrefixedURI(resourceURI);
					final String hash = UUID.randomUUID().toString();
					final GraphDatabaseService newResourceDB = loadResource(newResource, IMPERMANENT_GRAPH_DATABASE_PATH + hash + "-2",
							namespaceIndex);

					final Resource existingResource;
					final GDMResourceReader gdmReader;

					final TransactionHandler tx = new Neo4jTransactionHandler(permanentDatabase);

					if (optionalPrefixedContentSchema.isPresent() && optionalPrefixedContentSchema.get().getRecordIdentifierAttributePath() != null) {

						// determine legacy resource identifier via content schema
						final String recordIdentifier = GraphDBUtil.determineRecordIdentifier(newResourceDB, optionalPrefixedContentSchema.get()
								.getRecordIdentifierAttributePath(), prefixedResourceURI);

						// try to retrieve existing model via legacy record identifier
						// note: version is absent -> should make use of latest version
						gdmReader = new PropertyGraphGDMResourceByIDReader(recordIdentifier,
								optionalPrefixedContentSchema.get().getRecordIdentifierAttributePath(),
								prefixedDataModelURI, Optional.<Integer>absent(), permanentDatabase, tx, namespaceIndex);
					} else {

						// try to retrieve existing model via resource uri
						// note: version is absent -> should make use of latest version
						gdmReader = new PropertyGraphGDMResourceByURIReader(prefixedResourceURI, prefixedDataModelURI, Optional.<Integer>absent(),
								permanentDatabase, tx, namespaceIndex);
					}

					existingResource = gdmReader.read();

					if (existingResource == null) {

						// we don't need to calculate the delta, since everything is new

						shutDownDeltaDB(newResourceDB);

						// take new resource model, since there was no match in the data model graph for this resource identifier
						return Observable.just(newResource);
					}

					final String existingResourceURI = existingResource.getUri();
					final String prefixedExistingResourceURI = handler.getHandler().getProcessor().createPrefixedURI(existingResourceURI);
					final long existingResourceHash = handler.getHandler().getProcessor().generateResourceHash(prefixedExistingResourceURI,
							Optional.<String>absent());
					processedResources.add(existingResourceHash);

					// final Model newResourceModel = new Model();
					// newResourceModel.addResource(resource);

					final GraphDatabaseService existingResourceDB = loadResource(existingResource, IMPERMANENT_GRAPH_DATABASE_PATH + hash + "-1",
							namespaceIndex);

					final Changeset changeset = calculateDeltaForResource(existingResource, existingResourceDB, newResource, newResourceDB,
							optionalPrefixedContentSchema, namespaceIndex);

					if (!changeset.hasChanges()) {

						// process changeset only, if it provides changes

						GDMResource.LOG.debug("no changes detected for this resource");

						shutDownDeltaDBs(existingResourceDB, newResourceDB);

						return Observable.empty();
					}

					// write modified resources resource-wise - instead of the whole model at once.
					final GDMUpdateParser parser = new GDMChangesetParser(changeset, existingResourceHash, existingResourceDB,
							newResourceDB);
					parser.setGDMHandler(handler);
					parser.parse();

					shutDownDeltaDBs(existingResourceDB, newResourceDB);

					return Observable.empty();
				} catch (final DMPGraphException e) {

					throw new RuntimeException(e);
				}
			}
		});

		try {

			final Observable<Resource> completedNewResources = newResources.doOnCompleted(new Action0() {

				@Override
				public void call() {

					GDMResource.LOG.debug("finished calculating delta for model and writing changes to graph DB");
				}
			}).cache();

			@SuppressWarnings("unused") final Resource runNewResources =
					completedNewResources.toBlocking().lastOrDefault(null);

			final Observable<Long> processedResourcesObservable = Observable.from(processedResources);

			// return only model with new, non-existing resources
			return Tuple.tuple(completedNewResources, processedResourcesObservable);
		} catch (final RuntimeException e) {

			throw new DMPGraphException(e.getMessage(), e.getCause());
		}
	}

	private Changeset calculateDeltaForResource(final Resource existingResource, final GraphDatabaseService existingResourceDB,
			final Resource newResource, final GraphDatabaseService newResourceDB, final Optional<ContentSchema> optionalPrefixedContentSchema,
			final NamespaceIndex namespaceIndex)
			throws DMPGraphException {

		final String existingResourceURI = existingResource.getUri();
		final String newResourceURI = newResource.getUri();

		final String prefixedExistingResourceURI = namespaceIndex.createPrefixedURI(existingResourceURI);
		final String prefixedNewResourceURI = namespaceIndex.createPrefixedURI(newResourceURI);

		final long existingResourceHash = HashUtils.generateHash(prefixedExistingResourceURI);
		final long newResourceHash = HashUtils.generateHash(prefixedNewResourceURI);

		enrichModel(existingResourceDB, namespaceIndex, prefixedExistingResourceURI, existingResourceHash);
		enrichModel(newResourceDB, namespaceIndex, prefixedNewResourceURI, newResourceHash);

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

		final Optional<AttributePath> optionalPrefixedCommonAttributePath;

		if (optionalPrefixedContentSchema.isPresent()) {

			optionalPrefixedCommonAttributePath = AttributePathUtil.determineCommonAttributePath(optionalPrefixedContentSchema.get());
		} else {

			optionalPrefixedCommonAttributePath = Optional.absent();
		}

		if (optionalPrefixedCommonAttributePath.isPresent()) {

			// do specific processing with content schema knowledge

			final AttributePath commonPrefixedAttributePath = optionalPrefixedCommonAttributePath.get();
			final ContentSchema prefixedContentSchema = optionalPrefixedContentSchema.get();

			final Collection<CSEntity> newCSEntities = GraphDBUtil.getCSEntities(newResourceDB, prefixedNewResourceURI, commonPrefixedAttributePath,
					prefixedContentSchema);
			final Collection<CSEntity> existingCSEntities = GraphDBUtil.getCSEntities(existingResourceDB, prefixedExistingResourceURI,
					commonPrefixedAttributePath, prefixedContentSchema);

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
					Optional.fromNullable(newCSEntities), existingResourceDB, newResourceDB, prefixedExistingResourceURI, prefixedNewResourceURI);
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
					prefixedExistingResourceURI, prefixedNewResourceURI);
			firstDegreeExactCSValueMatcher.match();

			final Optional<? extends Collection<ValueEntity>> newExactCSValueNonMatches = firstDegreeExactCSValueMatcher.getNewEntitiesNonMatches();
			final Optional<? extends Collection<ValueEntity>> existingExactCSValueNonMatches = firstDegreeExactCSValueMatcher
					.getExistingEntitiesNonMatches();
			// 1.3 hash with key, value + entity order => matches value entities
			// 1.4 hash with key, value => matches value entities
			// 2. identify modifications for cs entities
			// 2.1 hash with key + entity order + value order => matches value entities
			final ModificationMatcher<ValueEntity> modificationCSMatcher = new FirstDegreeModificationCSValueMatcher(existingExactCSValueNonMatches,
					newExactCSValueNonMatches, existingResourceDB, newResourceDB, prefixedExistingResourceURI, prefixedNewResourceURI);
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
					prefixedExistingResourceURI, prefixedNewResourceURI);
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
					existingSubGraphLeafEntities, newSubGraphLeafEntities, existingResourceDB, newResourceDB, prefixedExistingResourceURI,
					prefixedNewResourceURI);
			firstDegreeExactSubGraphLeafEntityMatcher.match();

			final Optional<? extends Collection<SubGraphLeafEntity>> newFirstDegreeExactSubGraphLeafEntityNonMatches = firstDegreeExactSubGraphLeafEntityMatcher
					.getNewEntitiesNonMatches();
			final Optional<? extends Collection<SubGraphLeafEntity>> existingFirstDegreeExactSubGraphLeafEntityNonMatches = firstDegreeExactSubGraphLeafEntityMatcher
					.getExistingEntitiesNonMatches();
			// 7.3 identify modifications of (non-hierarchical) sub graphs
			final FirstDegreeModificationSubGraphLeafEntityMatcher firstDegreeModificationSubGraphLeafEntityMatcher = new FirstDegreeModificationSubGraphLeafEntityMatcher(
					existingFirstDegreeExactSubGraphLeafEntityNonMatches, newFirstDegreeExactSubGraphLeafEntityNonMatches, existingResourceDB,
					newResourceDB, prefixedExistingResourceURI, prefixedNewResourceURI);
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
		final Collection<ValueEntity> newFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(prefixedNewResourceURI, newResourceDB);
		final Collection<ValueEntity> existingFlatResourceNodeValueEntities = GraphDBUtil.getFlatResourceNodeValues(prefixedExistingResourceURI,
				existingResourceDB);
		// 3.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeExactGDMValueMatcher firstDegreeExactGDMValueMatcher = new FirstDegreeExactGDMValueMatcher(
				Optional.fromNullable(existingFlatResourceNodeValueEntities), Optional.fromNullable(newFlatResourceNodeValueEntities),
				existingResourceDB, newResourceDB, prefixedExistingResourceURI, prefixedNewResourceURI);
		firstDegreeExactGDMValueMatcher.match();

		final Optional<? extends Collection<ValueEntity>> newFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher
				.getNewEntitiesNonMatches();
		final Optional<? extends Collection<ValueEntity>> existingFirstDegreeExactGDMValueNonMatches = firstDegreeExactGDMValueMatcher
				.getExistingEntitiesNonMatches();
		// 4. identify modifications of resource node-based statements
		// 4.1 with key (predicate), value + value order => matches value entities
		final FirstDegreeModificationGDMValueMatcher firstDegreeModificationGDMValueMatcher = new FirstDegreeModificationGDMValueMatcher(
				existingFirstDegreeExactGDMValueNonMatches, newFirstDegreeExactGDMValueNonMatches, existingResourceDB, newResourceDB,
				prefixedExistingResourceURI, prefixedNewResourceURI);
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
		final boolean isExistingResourceMatchedCompletely = GraphDBUtil.checkGraphMatchingCompleteness(existingResourceDB, "existing resource");

		final boolean isNewResourceMatchedCompletely = GraphDBUtil.checkGraphMatchingCompleteness(newResourceDB, "new resource");

		if (!isExistingResourceMatchedCompletely && !isNewResourceMatchedCompletely) {

			throw new DMPGraphException("existing and new resource weren't matched completely by the delta algo");
		} else {

			if (!isExistingResourceMatchedCompletely) {

				throw new DMPGraphException("existing resource wasn't matched completely by the delta algo");
			}

			if (!isNewResourceMatchedCompletely) {

				throw new DMPGraphException("new resource wasn't matched completely by the delta algo");
			}
		}

		// traverse resource graphs to extract changeset
		final PropertyGraphDeltaGDMSubGraphWorker addedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(prefixedNewResourceURI,
				DeltaState.ADDITION, newResourceDB, namespaceIndex);
		final Map<Long, Statement> addedStatements = addedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker removedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(
				prefixedExistingResourceURI, DeltaState.DELETION, existingResourceDB, namespaceIndex);
		final Map<Long, Statement> removedStatements = removedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker newModifiedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(
				prefixedNewResourceURI,
				DeltaState.MODIFICATION, newResourceDB, namespaceIndex);
		final Map<Long, Statement> newModifiedStatements = newModifiedStatementsPGDGDMSGWorker.work();

		final PropertyGraphDeltaGDMSubGraphWorker existingModifiedStatementsPGDGDMSGWorker = new PropertyGraphDeltaGDMSubGraphWorker(
				prefixedExistingResourceURI, DeltaState.MODIFICATION, existingResourceDB, namespaceIndex);
		final Map<Long, Statement> existingModifiedStatements = existingModifiedStatementsPGDGDMSGWorker.work();

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

	private GraphDatabaseService loadResource(final Resource resource, final String impermanentGraphDatabaseDir, final NamespaceIndex namespaceIndex)
			throws DMPGraphException {

		// TODO: find proper graph database settings to hold everything in-memory only
		final GraphDatabaseService impermanentDB = impermanentGraphDatabaseFactory.newImpermanentDatabaseBuilder(impermanentGraphDatabaseDir)
				.setConfig(GraphDatabaseSettings.cache_type, "strong").newGraphDatabase();

		SchemaIndexUtils.createSchemaIndices(impermanentDB, impermanentGraphDatabaseDir);

		// TODO: implement handler that enriches the GDM resource with useful information for changeset detection
		final GDMHandler handler = new Neo4jDeltaGDMHandler(impermanentDB, namespaceIndex);

		final GDMParser parser = new GDMResourceParser(resource);
		parser.setGDMHandler(handler);
		parser.parse();

		LOG.debug("added '{}' statements ('{}' relationships; '{}' nodes; '{}' literals) to impermanent delta graph DB '{}'",
				handler.getCountedStatements(), handler.getRelationshipsAdded(), handler.getNodesAdded(), handler.getCountedLiterals(),
				impermanentGraphDatabaseDir);

		return impermanentDB;
	}

	private void enrichModel(final GraphDatabaseService graphDB, final NamespaceIndex namespaceIndex, final String prefixedResourceURI,
			final long resourceHash) throws DMPGraphException {

		final GDMWorker worker = new PropertyEnrichGDMWorker(prefixedResourceURI, resourceHash, graphDB, namespaceIndex);
		worker.work();
	}

	private void shutDownDeltaDBs(final GraphDatabaseService existingResourceDB, final GraphDatabaseService newResourceDB) {

		GDMResource.LOG.debug("start shutting down working graph data model DBs for resources");

		// should probably be delegated to a background worker thread, since it looks like that shutting down the working graph
		// DBs take some time (for whatever reason)
		final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
		service.submit(new Callable<Void>() {

			@Override
			public Void call() {

				newResourceDB.shutdown();
				existingResourceDB.shutdown();

				return null;
			}
		});

		GDMResource.LOG.debug("finished shutting down working graph data model DBs for resources");
	}

	private void shutDownDeltaDB(final GraphDatabaseService resourceDB) {

		GDMResource.LOG.debug("start shutting down working graph data model DB for resource");

		// should probably be delegated to a background worker thread, since it looks like that shutting down the working graph
		// DBs take some time (for whatever reason)
		final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
		service.submit(new Callable<Void>() {

			@Override
			public Void call() {

				resourceDB.shutdown();

				return null;
			}
		});

		GDMResource.LOG.debug("finished shutting down working graph data model DB for resource");
	}

	private Observable<Void> deprecateMissingRecords(final Observable<Long> processedResources, final String recordClassUri,
			final String dataModelUri,
			final int latestVersion, final GDMNeo4jProcessor processor) throws DMPGraphException {

		return processedResources.toList().map(new Func1<List<Long>, Void>() {

			@Override public Void call(final List<Long> processedResourcesSet) {

				// determine all record URIs of the data model
				// how? - via record class?

				try {

					processor.getProcessor().ensureRunningTx();

					final Label recordClassLabel = DynamicLabel.label(recordClassUri);

					final ResourceIterator<Node> recordNodes = processor.getProcessor().getDatabase()
							.findNodes(recordClassLabel, GraphStatics.DATA_MODEL_PROPERTY, dataModelUri);

					if (recordNodes == null) {

						GDMResource.LOG.debug("finished read data model record nodes TX successfully");

						return null;
					}

					final Set<Node> notProcessedResources = new HashSet<>();

					while (recordNodes.hasNext()) {

						final Node recordNode = recordNodes.next();

						final Long resourceHash = (Long) recordNode.getProperty(GraphStatics.HASH, null);

						if (resourceHash == null) {

							LOG.debug("there is no resource hash at record node '{}'", recordNode.getId());

							continue;
						}

						if (!processedResourcesSet.contains(resourceHash)) {

							notProcessedResources.add(recordNode);

							// TODO: do we also need to deprecate the record nodes themselves?
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

					return null;
				} catch (final Exception e) {

					final String message = "couldn't determine record URIs of the data model successfully";

					processor.getProcessor().failTx();

					GDMResource.LOG.error(message, e);

					throw new RuntimeException(message, e);
				}
			}
		});
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

		final BodyPart contentBodyPart = bodyParts.get(CONTENT_BODY_PART);

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

		final BodyPart metadataBodyPart = bodyParts.get(METADATA_BODY_PART);

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

			final String message = String.format("couldn't write GDM, couldn't deserialize metadata part '%s'", metadataString);

			GDMResource.LOG.error(message);

			throw new DMPGraphException(message, e);
		}
	}

	private Tuple<Observable<Resource>, BufferedInputStream> getModel(final InputStream content) {

		final BufferedInputStream bis = new BufferedInputStream(content, 1024);
		final ModelParser modelParser = new ModelParser(bis);

		return Tuple.tuple(modelParser.parse(), bis);
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

	private Optional<ContentSchema> getPrefixedContentSchema(final ObjectNode metadata, final NamespaceIndex namespaceIndex)
			throws DMPGraphException {

		final Optional<JsonNode> optionalContentSchemaJSON = getMetadataPartNode(DMPStatics.CONTENT_SCHEMA_IDENTIFIER, metadata, false);

		if (!optionalContentSchemaJSON.isPresent()) {

			return Optional.absent();
		}

		try {

			final String contentSchemaJSONString = objectMapper.writeValueAsString(optionalContentSchemaJSON.get());
			final ContentSchema contentSchema = objectMapper.readValue(contentSchemaJSONString, ContentSchema.class);

			final Optional<ContentSchema> contentSchemaOptional = Optional.fromNullable(contentSchema);

			if (contentSchemaOptional.isPresent()) {

				return prefixContentSchema(contentSchemaOptional.get(), namespaceIndex);
			}

			return contentSchemaOptional;
		} catch (final IOException e) {

			final String message = "could not deserialise content schema JSON for write from graph DB request";

			GDMResource.LOG.debug(message);

			return Optional.absent();
		}
	}

	private Optional<ContentSchema> prefixContentSchema(final ContentSchema contentSchema, final NamespaceIndex namespaceIndex)
			throws DMPGraphException {

		final Map<AttributePath, AttributePath> prefixedAttributePathMap = new HashMap<>();
		final Map<Attribute, Attribute> prefixedAttributeMap = new HashMap<>();

		final LinkedList<AttributePath> keyAttributePaths = contentSchema.getKeyAttributePaths();
		final LinkedList<AttributePath> prefixedKeyAttributePaths;

		if (keyAttributePaths != null) {

			prefixedKeyAttributePaths = new LinkedList<>();

			for (final AttributePath keyAttributePath : keyAttributePaths) {

				final AttributePath prefixedKeyAttributePath = prefixAttributePath(keyAttributePath, prefixedAttributePathMap, prefixedAttributeMap,
						namespaceIndex);
				prefixedKeyAttributePaths.add(prefixedKeyAttributePath);
			}
		} else {

			prefixedKeyAttributePaths = null;
		}

		final AttributePath recordIdentifierAttributePath = contentSchema.getRecordIdentifierAttributePath();
		final AttributePath prefixedRecordIdentifierAttributePath = prefixAttributePath(recordIdentifierAttributePath, prefixedAttributePathMap,
				prefixedAttributeMap, namespaceIndex);

		final AttributePath valueAttributePath = contentSchema.getValueAttributePath();

		final AttributePath prefixedValueAttributePath;

		if (valueAttributePath != null) {

			prefixedValueAttributePath = prefixAttributePath(valueAttributePath, prefixedAttributePathMap, prefixedAttributeMap,
					namespaceIndex);
		} else {

			prefixedValueAttributePath = null;
		}

		return Optional.of(new ContentSchema(prefixedRecordIdentifierAttributePath, prefixedKeyAttributePaths, prefixedValueAttributePath));
	}

	private AttributePath prefixAttributePath(final AttributePath attributePath, final NamespaceIndex namespaceIndex) throws DMPGraphException {

		final Map<AttributePath, AttributePath> prefixedAttributePathMap = new HashMap<>();
		final Map<Attribute, Attribute> prefixedAttributeMap = new HashMap<>();

		return prefixAttributePath(attributePath, prefixedAttributePathMap, prefixedAttributeMap, namespaceIndex);
	}

	private AttributePath prefixAttributePath(final AttributePath attributePath, final Map<AttributePath, AttributePath> prefixedAttributePathMap,
			final Map<Attribute, Attribute> prefixedAttributeMap, final NamespaceIndex namespaceIndex) throws DMPGraphException {

		if (!prefixedAttributeMap.containsKey(attributePath)) {

			final LinkedList<Attribute> attributes = attributePath.getAttributes();
			final LinkedList<Attribute> prefixedAttributes = new LinkedList<>();

			for (final Attribute attribute : attributes) {

				final Attribute prefixedAttribute = prefixAttribute(attribute, prefixedAttributeMap, namespaceIndex);
				prefixedAttributes.add(prefixedAttribute);
			}

			final AttributePath prefixedAttributePath = new AttributePath(prefixedAttributes);

			prefixedAttributePathMap.put(attributePath, prefixedAttributePath);
		}

		return prefixedAttributePathMap.get(attributePath);
	}

	private Attribute prefixAttribute(final Attribute attribute, final Map<Attribute, Attribute> prefixedAttributeMap,
			final NamespaceIndex namespaceIndex) throws DMPGraphException {

		if (!prefixedAttributeMap.containsKey(attribute)) {

			final String attributeUri = attribute.getUri();
			final String prefixedAttributeURI = namespaceIndex.createPrefixedURI(attributeUri);

			final Attribute prefixedAttribute = new Attribute(prefixedAttributeURI);

			prefixedAttributeMap.put(attribute, prefixedAttribute);
		}

		return prefixedAttributeMap.get(attribute);
	}

	/**
	 * default = false
	 *
	 * @param metadata
	 * @return
	 * @throws DMPGraphException
	 */
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

	/**
	 * default = true
	 *
	 * @param metadata
	 * @return
	 * @throws DMPGraphException
	 */
	private Optional<Boolean> getEnableVersioningFlag(final ObjectNode metadata) throws DMPGraphException {

		final Optional<String> optionalEnableVersioning = getMetadataPart(DMPStatics.ENABLE_VERSIONING_IDENTIFIER, metadata, false);

		final Optional<Boolean> result;

		if (optionalEnableVersioning.isPresent()) {

			result = Optional.fromNullable(Boolean.valueOf(optionalEnableVersioning.get()));
		} else {

			result = Optional.of(Boolean.TRUE);
		}

		return result;
	}

	private Optional<String> getStringValue(final String key, final JsonNode json) {

		final JsonNode node = json.get(key);
		final Optional<String> optionalValue;

		if (node != null) {

			optionalValue = Optional.fromNullable(node.asText());
		} else {

			optionalValue = Optional.absent();
		}

		return optionalValue;
	}

	private Optional<Integer> getIntValue(final String key, final JsonNode json) {

		final JsonNode node = json.get(key);
		final Optional<Integer> optionalValue;

		if (node != null) {

			optionalValue = Optional.fromNullable(node.asInt());
		} else {

			optionalValue = Optional.absent();
		}

		return optionalValue;
	}

	private ObjectNode deserializeJSON(final String jsonString, final String type) throws DMPGraphException {

		try {

			return objectMapper.readValue(jsonString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = String.format("could not deserialise request JSON for %s", type);

			GDMResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}
	}

	private String serializeJSON(final Object object, final String type) throws DMPGraphException {

		try {

			return objectMapper.writeValueAsString(object);
		} catch (final JsonProcessingException e) {

			final String message = String.format("some problems occur, while processing the JSON for %s", type);

			throw new DMPGraphException(message, e);
		}
	}

	private String readHeaders(final HttpHeaders httpHeaders) {

		final MultivaluedMap<String, String> headerParams = httpHeaders.getRequestHeaders();

		final StringBuilder sb = new StringBuilder();

		for (final Map.Entry<String, List<String>> entry : headerParams.entrySet()) {

			final String headerIdentifier = entry.getKey();
			final List<String> headerValues = entry.getValue();

			sb.append("\t\t").append(headerIdentifier).append(" = ");

			for (final String headerValue : headerValues) {

				sb.append(headerValue).append(", ");
			}

			sb.append("\n");
		}

		return sb.toString();
	}
}
