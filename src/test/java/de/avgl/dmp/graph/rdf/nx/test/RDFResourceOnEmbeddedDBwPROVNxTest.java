package de.avgl.dmp.graph.rdf.nx.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnEmbeddedDBwPROVNxTest extends RDFResourceDBwPROVNxTest {

	public RDFResourceOnEmbeddedDBwPROVNxTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
