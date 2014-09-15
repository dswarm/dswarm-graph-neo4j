package org.dswarm.graph.gdm.work;

import org.dswarm.graph.DMPGraphException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

/**
 *
 * @author tgaengler
 *
 */
public interface SubGraphNodeHandler {

	public void handleNode(Node node, Path path) throws DMPGraphException;
}
