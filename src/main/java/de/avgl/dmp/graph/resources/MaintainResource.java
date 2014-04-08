package de.avgl.dmp.graph.resources;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
@Path("/maintain")
public class MaintainResource {

	private static final Logger	LOG			= LoggerFactory.getLogger(MaintainResource.class);

	private static final long	chunkSize	= 50000;

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
	public Response cleanGraph(@Context final GraphDatabaseService database) throws IOException {

		MaintainResource.LOG.debug("start cleaning up the db");

		final long deleted = deleteSomeStatements(database);

		MaintainResource.LOG.debug("finished delete-all-entities TXs");

		MaintainResource.LOG.debug("start indices clean-up");

		// TODO: maybe separate index clean-up + observe index clean-up
		// => maybe we also need to do a label + relationship types clean-up ...

		deleteSomeIndices(database);

		MaintainResource.LOG.debug("finished indices clean-up");

		MaintainResource.LOG.debug("finished cleaning up the db");

		final StringWriter out = new StringWriter();
		JsonGenerator generator = jsonFactory.createGenerator(out);

		generator.writeStartObject();
		generator.writeNumberField("deleted", deleted);
		generator.writeEndObject();
		generator.flush();
		generator.close();

		return Response.ok(out.toString(), MediaType.APPLICATION_JSON_TYPE).build();
	}

	private long deleteSomeStatements(final GraphDatabaseService database) {

		final ExecutionEngine engine = new ExecutionEngine(database);

		final String deleteQuery = String.format(DELETE_CYPHER, MaintainResource.chunkSize);

		long deleted = 0;

		int i = 0;

		while (true) {

			i++;

			final Transaction tx = database.beginTx();

			MaintainResource.LOG.debug("try to delete up to " + MaintainResource.chunkSize + " nodes and their relationships for the " + i
					+ ". time");

			try {

				final ExecutionResult result = engine.execute(deleteQuery);

				if (!result.iterator().hasNext()) {

					break;
				}

				final Map<String, Object> row = result.iterator().next();

				if (row == null || row.isEmpty()) {

					break;
				}

				final Entry<String, Object> entry = row.entrySet().iterator().next();

				if (entry == null) {

					break;
				}

				final Object value = entry.getValue();

				if (value == null) {

					break;
				}

				if (!entry.getKey().equals("entity_count")) {

					break;
				}

				final Long count = (Long) value;

				deleted += count;

				MaintainResource.LOG.debug("deleted " + count + " entities");

				if (count == 0) {

					break;
				}

				tx.success();

			} catch (final Exception e) {

				MaintainResource.LOG.error("couldn't finished delete-all-entities TX successfully", e);

				tx.failure();
			} finally {

				MaintainResource.LOG.debug("finished delete-all-entities TX finally");

				tx.close();
			}
		}

		return deleted;
	}

	private void deleteSomeIndices(final GraphDatabaseService database) {
		MaintainResource.LOG.debug("start delete indices TX");

		final Transaction itx = database.beginTx();

		try {

			final Index<Node> resources = database.index().forNodes("resources");
			final Index<Node> resourcesWProvenance = database.index().forNodes("resources_w_provenance");
			final Index<Node> resourceTypes = database.index().forNodes("resource_types");
			final Index<Relationship> statements = database.index().forRelationships("statements");

			if (resources != null) {

				MaintainResource.LOG.debug("delete resources index");

				resources.delete();
			}

			if (resources != null) {

				MaintainResource.LOG.debug("delete resources with provenance index");

				resourcesWProvenance.delete();
			}

			if (resourceTypes != null) {

				MaintainResource.LOG.debug("delete resource types index");

				resourceTypes.delete();
			}

			if (statements != null) {

				MaintainResource.LOG.debug("delete statements index");

				statements.delete();
			}

			itx.success();

		} catch (final Exception e) {

			MaintainResource.LOG.error("couldn't finished delete indices TX successfully", e);

			itx.failure();
		} finally {

			MaintainResource.LOG.debug("finished delete indices TX finally");


			itx.close();
		}
	}
}
