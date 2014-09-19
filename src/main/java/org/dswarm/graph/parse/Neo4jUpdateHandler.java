package org.dswarm.graph.parse;

import org.neo4j.graphdb.Relationship;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.versioning.VersionHandler;

/**
 * @author tgaengler
 */
public interface Neo4jUpdateHandler extends Neo4jHandler {

	public void deprecateStatement(final long index);

	public Relationship deprecateStatement(final String uuid) throws DMPGraphException;

	public VersionHandler getVersionHandler();
}
