package org.dswarm.graph.rdf.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

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
