package org.dswarm.graph.gdm.work;

import org.dswarm.graph.DMPGraphException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

/**
 * @author tgaengler
 */
public interface SubGraphRelationshipHandler {

	public void handleRelationship(Relationship rel, Path path) throws DMPGraphException;
}
