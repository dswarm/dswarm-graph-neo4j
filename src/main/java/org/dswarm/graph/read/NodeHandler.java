package org.dswarm.graph.read;

import org.dswarm.graph.DMPGraphException;
import org.neo4j.graphdb.Node;


/**
 *
 * @author tgaengler
 *
 */
public interface NodeHandler {

	public void handleNode(Node node) throws DMPGraphException;
}
