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

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.DMPStatics;
import org.dswarm.common.types.Tuple;
import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.deprecate.DataModelNeo4jDeprecator;
import org.dswarm.graph.deprecate.RecordsNeo4jDeprecator;
import org.dswarm.graph.deprecate.RelationshipDeprecator;
import org.dswarm.graph.index.MapDBUtils;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.index.SchemaIndexUtils;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.Neo4jTransactionHandler;
import org.dswarm.graph.tx.TransactionHandler;
import org.dswarm.graph.utils.GraphDatabaseUtils;
import org.dswarm.graph.utils.NamespaceUtils;

/**
 * @author tgaengler
 */
@Path("/maintain")
public class MaintainResource extends GraphResource {

	private static final Logger LOG = LoggerFactory.getLogger(MaintainResource.class);

	private static final String PERSISTENT_GRAPH_DATABASE_IDENTIFIER = "persistent graph database";

	private static final long chunkSize = 50000;

	// TODO: maybe divide this into 2 queries and without OPTIONAL
	private static final String DELETE_CYPHER = "MATCH (a) WITH a LIMIT %d OPTIONAL MATCH (a)-[r]-() DELETE a,r RETURN COUNT(*) AS entity_count";

	private static final String DEPRECATE_DATA_MODEL_TYPE = "deprecate data model";

	private static final String DEPRECATE_RECORDS_TYPE = "deprecate records";

	public MaintainResource() {

	}

	@GET
	@Path("/ping")
	public String ping() {

		MaintainResource.LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Path("/deprecate/datamodel")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deprecateDataModel(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		MaintainResource.LOG.info("try to deprecate data model in graph db");

		final ObjectNode requestJSON = deserializeJSON(jsonObjectString, DEPRECATE_DATA_MODEL_TYPE);

		final String dataModelUri = requestJSON.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		final String prefixedDataModelUri = namespaceIndex.createPrefixedURI(dataModelUri);

		MaintainResource.LOG.info("try to deprecate statements in data model '{}' ('{}') in graph db", dataModelUri, prefixedDataModelUri);

		final BasicNeo4jProcessor processor = new DataModelNeo4jProcessor(database, tx, namespaceIndex, prefixedDataModelUri);

		final RelationshipDeprecator dataModelDeprecator = new DataModelNeo4jDeprecator(processor, true, prefixedDataModelUri);

		dataModelDeprecator.work();

		final int relationshipsDeprecated = dataModelDeprecator.getRelationshipsDeprecated();

		if (relationshipsDeprecated > 0) {

			// update data model version only when some statements are deprecated the DB
			dataModelDeprecator.getVersionHandler().updateLatestVersion();
		}

		dataModelDeprecator.closeTransaction();

		LOG.info("deprecated '{}' relationships in data model '{}' ('{}') in graph db", relationshipsDeprecated, dataModelUri, prefixedDataModelUri);

		final ObjectNode resultJSON = simpleObjectMapper.createObjectNode();
		resultJSON.put("deprecated", relationshipsDeprecated);
		final String result = serializeJSON(resultJSON, DEPRECATE_DATA_MODEL_TYPE);

		return Response.ok(result, MediaType.APPLICATION_JSON_TYPE).build();
	}

	@POST
	@Path("/deprecate/records")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deprecateRecords(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		MaintainResource.LOG.info("try to deprecate records in a data model in graph db");

		final ObjectNode requestJSON = deserializeJSON(jsonObjectString, DEPRECATE_RECORDS_TYPE);

		final String dataModelUri = requestJSON.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();

		final Collection<String> recordURIs = getRecordURIs(requestJSON);

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		final String prefixedDataModelUri = namespaceIndex.createPrefixedURI(dataModelUri);

		final Collection<String> prefixedRecordURIs = prefixRecordURIs(recordURIs, namespaceIndex);

		MaintainResource.LOG.info("try to deprecate '{}' records in data model '{}' ('{}') in graph db", prefixedRecordURIs.size(), dataModelUri,
				prefixedDataModelUri);

		final BasicNeo4jProcessor processor = new DataModelNeo4jProcessor(database, tx, namespaceIndex, prefixedDataModelUri);

		final RelationshipDeprecator recordsDeprecator = new RecordsNeo4jDeprecator(processor, true, prefixedDataModelUri, prefixedRecordURIs);

		recordsDeprecator.work();

		final int relationshipsDeprecated = recordsDeprecator.getRelationshipsDeprecated();

		if (relationshipsDeprecated > 0) {

			// update data model version only when some statements are deprecated the DB
			recordsDeprecator.getVersionHandler().updateLatestVersion();
		}

		recordsDeprecator.closeTransaction();

		LOG.info("deprecated '{}' records with '{}' relationships in data model '{}' ('{}') in graph db", prefixedRecordURIs.size(),
				relationshipsDeprecated, dataModelUri, prefixedDataModelUri);

		final ObjectNode resultJSON = simpleObjectMapper.createObjectNode();
		resultJSON.put("deprecated", relationshipsDeprecated);
		final String result = serializeJSON(resultJSON, DEPRECATE_DATA_MODEL_TYPE);

		return Response.ok(result, MediaType.APPLICATION_JSON_TYPE).build();

	}

	/**
	 * note utilise this endpoint with care, because it cleans your complete db!
	 *
	 * @param database the graph database
	 */
	@DELETE
	@Path("/delete")
	@Produces("application/json")
	public Response cleanGraph(@Context final GraphDatabaseService database) throws IOException, DMPGraphException {

		MaintainResource.LOG.debug("start cleaning up the db");

		final long deleted = deleteSomeStatements(database);

		MaintainResource.LOG.debug("finished delete-all-entities TXs");

		MaintainResource.LOG.debug("start legacy indices clean-up");

		// TODO: maybe separate index clean-up + observe index clean-up
		// => maybe we also need to do a label + relationship types clean-up ... => this is not supported right now ...

		deleteSomeLegacyIndices(database);

		MaintainResource.LOG.debug("finished legacy indices clean-up");

		MaintainResource.LOG.debug("start schema indices clean-up");

		deleteSomeSchemaIndices(database);

		MaintainResource.LOG.debug("finished schema indices clean-up");

		MaintainResource.LOG.debug("finished cleaning up the db");

		final StringWriter out = new StringWriter();
		final JsonGenerator generator = simpleObjectMapper.getFactory().createGenerator(out);

		generator.writeStartObject();
		generator.writeNumberField("deleted", deleted);
		generator.writeEndObject();
		generator.flush();
		generator.close();

		final String result = out.toString();

		out.flush();
		out.close();

		return Response.ok(result, MediaType.APPLICATION_JSON_TYPE).build();
	}

	@POST
	@Path("/schemaindices")
	public Response createSchemaIndices(@Context final GraphDatabaseService database) throws DMPGraphException {

		SchemaIndexUtils.createSchemaIndices(database, PERSISTENT_GRAPH_DATABASE_IDENTIFIER);

		initPrefixes(database);

		return Response.ok().build();
	}

	@POST
	@Path("/initprefixcounter")
	public Response initPrefixCounter(@Context final GraphDatabaseService database) throws DMPGraphException {

		initPrefixCounterInternal(database);

		return Response.ok().build();
	}

	private long deleteSomeStatements(final GraphDatabaseService database) throws DMPGraphException {

		final String deleteQuery = String.format(DELETE_CYPHER, MaintainResource.chunkSize);

		long deleted = 0;

		int i = 0;

		while (true) {

			i++;

			try (final Transaction tx = database.beginTx()) {

				MaintainResource.LOG
						.debug("try to delete up to {} nodes and their relationships for the {}. time", MaintainResource.chunkSize, i);

				final Result result = database.execute(deleteQuery);

				if (result == null) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. result is empty");

					tx.success();
					tx.close();

					break;
				}

				if (!result.hasNext()) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. result iterator is empty");

					result.close();
					tx.success();
					tx.close();

					break;
				}

				final Map<String, Object> row = result.next();

				if (row == null || row.isEmpty()) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. row map is empty");

					result.close();
					tx.success();
					tx.close();

					break;
				}

				final Entry<String, Object> entry = row.entrySet().iterator().next();

				if (entry == null) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. entry is not available");

					result.close();
					tx.success();
					tx.close();

					break;
				}

				final Object value = entry.getValue();

				if (value == null) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. value is not available");

					result.close();
					tx.success();
					tx.close();

					break;
				}

				if (!entry.getKey().equals("entity_count")) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. entity count is not available");

					result.close();
					tx.success();
					tx.close();

					break;
				}

				final Long count = (Long) value;

				deleted += count;

				MaintainResource.LOG.debug("deleted {} entities", count);

				if (count < chunkSize) {

					MaintainResource.LOG.debug("there are no more results for removal available, i.e. current result is smaller than chunk size");

					result.close();
					tx.success();

					break;
				}

				result.close();
				tx.success();
				tx.close();
			} catch (final Exception e) {

				final String message = "couldn't finish delete-all-entities TX successfully";

				MaintainResource.LOG.error(message, e);

				throw new DMPGraphException(message);
			}
		}

		return deleted;
	}

	private void deleteSomeLegacyIndices(final GraphDatabaseService database) throws DMPGraphException {

		MaintainResource.LOG.debug("start delete legacy indices TX");

		try (final Transaction itx = database.beginTx()) {

			final Index<Node> resources = database.index().forNodes(GraphIndexStatics.RESOURCES_INDEX_NAME);
			final Index<Node> values = database.index().forNodes(GraphIndexStatics.VALUES_INDEX_NAME);
			final Index<Node> resourcesWDataModel = database.index().forNodes(GraphIndexStatics.RESOURCES_W_DATA_MODEL_INDEX_NAME);
			final Index<Node> resourceTypes = database.index().forNodes(GraphIndexStatics.RESOURCE_TYPES_INDEX_NAME);
			final Tuple<Set<Long>, DB> statementHashesMapDBIndexTuple = getOrCreateLongIndex(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME, database);
			final Index<Relationship> statementUUIDs = database.index().forRelationships(GraphIndexStatics.STATEMENT_UUIDS_INDEX_NAME);

			if (resources != null) {

				MaintainResource.LOG.debug("delete {} legacy index", GraphIndexStatics.RESOURCES_INDEX_NAME);

				resources.delete();
			}

			if (resourcesWDataModel != null) {

				MaintainResource.LOG.debug("delete {} legacy index", GraphIndexStatics.RESOURCES_W_DATA_MODEL_INDEX_NAME);

				resourcesWDataModel.delete();
			}

			if (resourceTypes != null) {

				MaintainResource.LOG.debug("delete {} legacy index", GraphIndexStatics.RESOURCE_TYPES_INDEX_NAME);

				resourceTypes.delete();
			}

			final DB mapDB = statementHashesMapDBIndexTuple.v2();

			if (mapDB.exists(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME)) {

				MaintainResource.LOG.debug("delete {} mapdb index", GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);

				mapDB.delete(GraphIndexStatics.STATEMENT_HASHES_INDEX_NAME);
				mapDB.commit();
				mapDB.close();
			}

			if (statementUUIDs != null) {

				MaintainResource.LOG.debug("delete {} legacy index", GraphIndexStatics.STATEMENT_UUIDS_INDEX_NAME);

				statementUUIDs.delete();
			}

			if (values != null) {

				MaintainResource.LOG.debug("delete {} legacy index", GraphIndexStatics.VALUES_INDEX_NAME);

				values.delete();
			}

			itx.success();
			itx.close();
		} catch (final Exception e) {

			final String message = "couldn't finish delete legacy indices TX successfully";

			MaintainResource.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		MaintainResource.LOG.debug("finished delete legacy indices TX");
	}

	private void deleteSomeSchemaIndices(final GraphDatabaseService database) throws DMPGraphException {

		MaintainResource.LOG.debug("start delete schema indices TX");

		try (final Transaction itx = database.beginTx()) {

			final Schema schema = database.schema();

			if (schema == null) {

				MaintainResource.LOG.debug("no schema available");

				itx.success();
				itx.close();

				return;
			}

			final Iterable<IndexDefinition> indexDefinitions = schema.getIndexes();

			if (indexDefinitions == null) {

				MaintainResource.LOG.debug("no schema indices available");

				itx.success();
				itx.close();

				return;
			}

			for (final IndexDefinition indexDefinition : indexDefinitions) {

				MaintainResource.LOG.debug("drop '{}' : '{}' schema index", indexDefinition.getLabel().name(),
						indexDefinition.getPropertyKeys().iterator().next());

				indexDefinition.drop();
			}

			itx.success();
			itx.close();
		} catch (final Exception e) {

			final String message = "couldn't finish delete schema indices TX successfully";

			MaintainResource.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		MaintainResource.LOG.debug("finished delete schema indices TX");
	}

	private Tuple<Set<Long>, DB> getOrCreateLongIndex(final String name, final GraphDatabaseService database) throws IOException {

		final String storeDir = GraphDatabaseUtils.determineMapDBIndexStoreDir(database);

		// storeDir + File.separator + MapDBUtils.INDEX_DIR + name
		return MapDBUtils.createOrGetPersistentLongIndexTreeSetGlobalTransactional(storeDir + File.separator + name, name);
	}

	private void initPrefixes(final GraphDatabaseService database) throws DMPGraphException {

		MaintainResource.LOG.debug("start initialising namespaces index");

		final TransactionHandler tx = new Neo4jTransactionHandler(database);
		final NamespaceIndex namespaceIndex = new NamespaceIndex(database, tx);

		tx.ensureRunningTx();

		try {

			final URL prefixesFileURL = Resources.getResource("prefixes.json");
			final String prefixesJSONString = Resources.toString(prefixesFileURL, Charsets.UTF_8);
			final Map<String, String> prefixesNamespacesMap = simpleObjectMapper
					.readValue(prefixesJSONString, new TypeReference<HashMap<String, String>>() {

					});

			for (final Map.Entry<String, String> entry : prefixesNamespacesMap.entrySet()) {

				final String prefix = entry.getKey();
				final String namespace = entry.getValue();

				final Optional<Node> optionalPrefix = NamespaceUtils.getPrefix(namespace, database);

				if (!optionalPrefix.isPresent()) {

					namespaceIndex.addPrefix(namespace, prefix);
				} else {

					final String prefixFromDB = (String) optionalPrefix.get().getProperty(GraphProcessingStatics.PREFIX_PROPERTY);

					MaintainResource.LOG
							.debug("prefix '{}' is already available for namespace '{}', i.e., no further entry with prefix '{}' will be created",
									prefixFromDB, namespace, prefix);
				}
			}

			tx.succeedTx();
		} catch (final Exception e) {

			tx.failTx();

			final String message = "couldn't initialize prefixes successfully";

			LOG.error(message);

			throw new DMPGraphException(message, e);
		}

		MaintainResource.LOG.debug("finished initialising namespaces index");
	}

	private Collection<String> getRecordURIs(final ObjectNode json) {

		final JsonNode recordsNode = json.get(DMPStatics.RECORDS_IDENTIFIER);

		final ArrayList<String> recordURIs = new ArrayList<>();

		for (final JsonNode recordNode : recordsNode) {

			final String recordURI = recordNode.asText();

			recordURIs.add(recordURI);
		}

		return recordURIs;
	}

	private Collection<String> prefixRecordURIs(final Collection<String> recordURIs, final NamespaceIndex namespaceIndex) throws DMPGraphException {

		final ArrayList<String> prefixedRecordURIs = new ArrayList<>();

		for (final String recordURI : recordURIs) {

			final String prefixedRecordURI = namespaceIndex.createPrefixedURI(recordURI);

			prefixedRecordURIs.add(prefixedRecordURI);
		}

		return prefixedRecordURIs;
	}

	private void initPrefixCounterInternal(final GraphDatabaseService database) throws DMPGraphException {

		try (final Transaction tx = database.beginTx()) {

			final ResourceIterator<Node> nodes = database.findNodes(GraphProcessingStatics.PREFIX_LABEL);

			if (nodes == null) {

				LOG.debug("no prefix nodes available - prefix does not need to initialised with a specific value");

				tx.success();
				tx.close();

				return;
			}

			long biggestPrefixNumber = 0;

			while (nodes.hasNext()) {

				final Node prefixNode = nodes.next();

				final Object prefixObject = prefixNode.getProperty(GraphProcessingStatics.PREFIX_PROPERTY, null);

				if (prefixObject == null) {

					LOG.debug("could not find prefix at prefix node '{}'", prefixNode.getId());

					continue;
				}

				final String prefix = (String) prefixObject;

				if (!prefix.startsWith(NamespaceUtils.NAMESPACE_PREFIX_BASE)) {

					// namespace is no candidate for prefix counter number

					continue;
				}

				if (!(prefix.length() > 2)) {

					// namespace is no candidate for prefix counter number

					continue;
				}

				final String prefixNumberString = prefix.substring(2, prefix.length());

				try {

					final Long prefixNumber = Long.valueOf(prefixNumberString);

					if (prefixNumber > biggestPrefixNumber) {

						biggestPrefixNumber = prefixNumber;
					}
				} catch (final NumberFormatException e) {

					LOG.debug("could not convert prefix number string '{}' to a number", prefixNumberString);
				}
			}

			nodes.close();

			// create persistent prefix counter with biggest given prefix number + 1
			biggestPrefixNumber = biggestPrefixNumber + 1;

			final String storeDir = GraphDatabaseUtils.determineMapDBIndexStoreDir(database);

			final Tuple<Atomic.Long, DB> prefixCounterTuple = MapDBUtils
					.createOrGetPersistentLongIndexGlobalTransactional(storeDir + File.separator + GraphIndexStatics.PREFIX_COUNTER_INDEX_NAME,
							GraphIndexStatics.PREFIX_COUNTER_INDEX_NAME, biggestPrefixNumber);
			final DB prefixCounterDB = prefixCounterTuple.v2();

			prefixCounterDB.commit();
			prefixCounterDB.close();

			LOG.info("initialized the prefix counter index with '{}'", biggestPrefixNumber);

			tx.success();
			tx.close();
		} catch (final Exception e) {

			final String message = "couldn't finish init prefix counter TX successfully";

			MaintainResource.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}
}
