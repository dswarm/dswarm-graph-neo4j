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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.mapdb.DB;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.types.Tuple;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.index.MapDBUtils;
import org.dswarm.graph.index.SchemaIndexUtils;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.utils.GraphDatabaseUtils;

/**
 * @author tgaengler
 */
@Path("/maintain")
public class MaintainResource {

	private static final Logger LOG = LoggerFactory.getLogger(MaintainResource.class);

	private static final long chunkSize = 50000;

	// TODO: maybe divide this into 2 queries and without OPTIONAL
	private static final String DELETE_CYPHER = "MATCH (a) WITH a LIMIT %d OPTIONAL MATCH (a)-[r]-() DELETE a,r RETURN COUNT(*) AS entity_count";

	private static final JsonFactory jsonFactory = new JsonFactory();

	public MaintainResource() {

	}

	@GET
	@Path("/ping")
	public String ping() {

		MaintainResource.LOG.debug("ping was called");

		return "pong";
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
		final JsonGenerator generator = jsonFactory.createGenerator(out);

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

		getOrCreateIndex(BasicNeo4jProcessor.RESOURCE_LABEL, GraphStatics.URI_PROPERTY, database);
		getOrCreateIndex(BasicNeo4jProcessor.RESOURCE_LABEL, GraphStatics.HASH, database);
		getOrCreateIndex(BasicNeo4jProcessor.RESOURCE_TYPE_LABEL, GraphStatics.URI_PROPERTY, database);
		getOrCreateIndex(BasicNeo4jProcessor.LITERAL_LABEL, GraphStatics.VALUE_PROPERTY, database);

		return Response.ok().build();
	}

	private void getOrCreateIndex(final Label label, final String property, final GraphDatabaseService database) throws DMPGraphException {

		final IndexDefinition indexDefinition = SchemaIndexUtils.getOrCreateIndex(label, property, database);

		if (indexDefinition == null) {

			throw new DMPGraphException(
					String.format("something went wrong while index determination/creation for label '%s' and property '%s'", label.name(),
							property));
		}
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
}
