package de.avgl.dmp.graph.rdf.nx.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnEmbeddedDBNxTest extends RDFResourceDBNxTest {

	public RDFResourceOnEmbeddedDBNxTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
