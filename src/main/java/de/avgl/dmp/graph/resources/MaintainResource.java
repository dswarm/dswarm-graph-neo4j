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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
@Path("/maintain")
public class MaintainResource {

	private static final Logger	LOG	= LoggerFactory.getLogger(MaintainResource.class);

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

		final ExecutionEngine engine = new ExecutionEngine(database);

		final String deleteQuery = "MATCH (a)\n" + "WITH a\n" + "LIMIT 10000\n" + "OPTIONAL MATCH (a)-[r]-()\n" + "DELETE a,r\n"
				+ "RETURN COUNT(*) AS entity_count\n";

		// TODO: should we do this in a transaction?

		ExecutionResult result = engine.execute(deleteQuery);

		MaintainResource.LOG.debug("try to delete up to 10000 nodes and their relationships for the first time");

		int i = 2;

		while (result != null && result.iterator().hasNext()) {

			final Map<String, Object> row = result.iterator().next();

			if (row == null) {

				break;
			}

			if (row.isEmpty()) {

				break;
			}

			final Set<Entry<String, Object>> column = row.entrySet();

			if (column == null) {

				break;
			}

			if (column.isEmpty()) {

				break;
			}

			final Entry<String, Object> entry = column.iterator().next();

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
			
			MaintainResource.LOG.debug("deleted " + count + " entities");

			if (count.longValue() < 10000) {

				break;
			}

			MaintainResource.LOG.debug("try to delete up to 10000 nodes and their relationships for the " + i + " time");

			result = engine.execute(deleteQuery);

			i++;
		}

		MaintainResource.LOG.debug("finished cleaning up the db");

		return Response.ok().build();
	}
}
