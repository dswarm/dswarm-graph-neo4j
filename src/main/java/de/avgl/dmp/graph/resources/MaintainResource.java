package de.avgl.dmp.graph.resources;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

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
	 * @param database
	 * @return
	 */
	@DELETE
	@Path("/delete")
	public Response cleanGraph(@Context final GraphDatabaseService database) {

		MaintainResource.LOG.debug("start cleaning up the db");

		deleteSomeStatements(database);

		MaintainResource.LOG.debug("finished delete-all-entities TXs");

		MaintainResource.LOG.debug("start indices clean-up");

		// TODO: maybe separate index clean-up + observe index clean-up
		// => maybe we also need to do a label + relationship types clean-up ...

		final Transaction itx = database.beginTx();

		MaintainResource.LOG.debug("start delete indices TX");

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
		} catch (final Exception e) {

			MaintainResource.LOG.error("couldn't finished delete indices TX successfully", e);

			itx.failure();
			itx.close();
		} finally {

			MaintainResource.LOG.debug("finished delete indices TX finally");

			itx.success();
			itx.close();
		}

		MaintainResource.LOG.debug("finished indices clean-up");

		MaintainResource.LOG.debug("finished cleaning up the db");

		return Response.ok().build();
	}

	private void deleteSomeStatements(final GraphDatabaseService database) {

		final ExecutionEngine engine = new ExecutionEngine(database);

		final String deleteQuery = "MATCH (a)\n" + "WITH a\n" + "LIMIT " + MaintainResource.chunkSize + "\n" + "OPTIONAL MATCH (a)-[r]-()\n"
				+ "DELETE a,r\n" + "RETURN COUNT(*) AS entity_count\n";

		boolean finished = false;

		while (!finished) {

			final Transaction tx = database.beginTx();

			try {

				final ExecutionResult result = engine.execute(deleteQuery);

				MaintainResource.LOG.debug("try to delete up to " + MaintainResource.chunkSize + " nodes and their relationships for the first time");

				int i = 1;

				if (result == null) {

					finished = true;

					break;
				}

				if (!result.iterator().hasNext()) {

					finished = true;

					break;
				}

				final Map<String, Object> row = result.iterator().next();

				if (row == null) {

					finished = true;

					break;
				}

				if (row.isEmpty()) {

					finished = true;

					break;
				}

				final Set<Entry<String, Object>> column = row.entrySet();

				if (column == null) {

					finished = true;

					break;
				}

				if (column.isEmpty()) {

					finished = true;

					break;
				}

				final Entry<String, Object> entry = column.iterator().next();

				if (entry == null) {

					finished = true;

					break;
				}

				final Object value = entry.getValue();

				if (value == null) {

					finished = true;

					break;
				}

				if (!entry.getKey().equals("entity_count")) {

					finished = true;

					break;
				}

				final Long count = (Long) value;

				MaintainResource.LOG.debug("deleted " + count + " entities");

				if (count.longValue() < MaintainResource.chunkSize) {

					finished = true;

					break;
				}

				MaintainResource.LOG.debug("try to delete up to " + MaintainResource.chunkSize + " nodes and their relationships for the " + i
						+ " time");

				i++;
			} catch (final Exception e) {

				MaintainResource.LOG.error("couldn't finished delete-all-entities TX successfully", e);

				tx.failure();
				tx.close();
			} finally {

				MaintainResource.LOG.debug("finished delete-all-entities TX finally");

				tx.success();
				tx.close();
			}
		}
	}
}
