package org.dswarm.graph.read;

import org.dswarm.graph.DMPGraphException;
import org.neo4j.graphdb.Relationship;

/**
 * @author tgaengler
 */
public interface RelationshipHandler {

	public void handleRelationship(Relationship rel) throws DMPGraphException;
}
