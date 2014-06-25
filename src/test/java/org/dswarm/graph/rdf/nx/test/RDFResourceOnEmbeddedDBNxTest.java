package org.dswarm.graph.rdf.nx.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnEmbeddedDBNxTest extends RDFResourceDBNxTest {

	public RDFResourceOnEmbeddedDBNxTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
