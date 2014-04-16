package de.avgl.dmp.graph.rdf.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * 
 * @author tgaengler
 *
 */
public class RDFResourceOnEmbeddedDBTest extends RDFResourceTest {

	public RDFResourceOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
