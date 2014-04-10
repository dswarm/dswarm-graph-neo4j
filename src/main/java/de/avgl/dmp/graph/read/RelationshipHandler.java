package de.avgl.dmp.graph.read;

import org.neo4j.graphdb.Relationship;

import de.avgl.dmp.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public interface RelationshipHandler {

	public void handleRelationship(Relationship rel) throws DMPGraphException;
}
