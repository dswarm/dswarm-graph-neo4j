package de.avgl.dmp.graph.rdf.read;

import org.neo4j.graphdb.Node;


/**
 * 
 * @author tgaengler
 *
 */
public interface NodeHandler {

	public void handleNode(Node node);
}
