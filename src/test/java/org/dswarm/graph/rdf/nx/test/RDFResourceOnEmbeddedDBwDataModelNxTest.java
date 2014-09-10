package org.dswarm.graph.rdf.nx.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnEmbeddedDBwDataModelNxTest extends RDFResourceDBwDataModelNxTest {

	public RDFResourceOnEmbeddedDBwDataModelNxTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
