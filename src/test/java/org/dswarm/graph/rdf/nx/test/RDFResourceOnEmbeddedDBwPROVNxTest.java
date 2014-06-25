package org.dswarm.graph.rdf.nx.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnEmbeddedDBwPROVNxTest extends RDFResourceDBwPROVNxTest {

	public RDFResourceOnEmbeddedDBwPROVNxTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
