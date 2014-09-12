package org.dswarm.graph.gdm.work;

import org.dswarm.graph.DMPGraphException;
import org.neo4j.graphdb.Node;

/**
 *
 * @author tgaengler
 *
 */
public interface HierarchyLevelNodeHandler {

	public void handleNode(Node node, int hierarchyLevel) throws DMPGraphException;
}
