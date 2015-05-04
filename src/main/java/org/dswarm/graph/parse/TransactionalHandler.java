package org.dswarm.graph.parse;

import org.neo4j.graphdb.GraphDatabaseService;

import org.dswarm.graph.index.NamespaceIndex;

/**
 * @author tgaengler
 */
public interface TransactionalHandler extends Handler {

	GraphDatabaseService getDatabase();

	NamespaceIndex getNamespaceIndex();
}
