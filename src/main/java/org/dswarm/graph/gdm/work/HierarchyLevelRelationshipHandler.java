package org.dswarm.graph.gdm.work;

import org.dswarm.graph.DMPGraphException;
import org.neo4j.graphdb.Relationship;

/**
 * @author tgaengler
 */
public interface HierarchyLevelRelationshipHandler {

	public void handleRelationship(Relationship rel, int hierarchyLevel) throws DMPGraphException;
}
