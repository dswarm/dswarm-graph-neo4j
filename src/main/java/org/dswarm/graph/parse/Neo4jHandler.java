package org.dswarm.graph.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.Statement;

/**
 * @author tgaengler
 */
public interface Neo4jHandler {

	public void handleStatement(final Statement statement) throws DMPGraphException;

	public void setResourceUri(final String resourceUri);

	public void closeTransaction() throws DMPGraphException;

	public long getCountedStatements();

	public int getRelationshipsAdded();
}
