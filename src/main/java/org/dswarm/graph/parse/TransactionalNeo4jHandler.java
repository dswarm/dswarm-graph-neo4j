package org.dswarm.graph.parse;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public interface TransactionalNeo4jHandler extends Neo4jHandler {

	GraphDatabaseService getDatabase();
}
