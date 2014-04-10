package de.avgl.dmp.graph.read;

import org.neo4j.graphdb.Node;

import de.avgl.dmp.graph.DMPGraphException;


/**
 * 
 * @author tgaengler
 *
 */
public interface NodeHandler {

	public void handleNode(Node node) throws DMPGraphException;
}
