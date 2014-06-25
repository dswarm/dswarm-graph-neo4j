package org.dswarm.graph.rdf.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class RDFResourceOnEmbedded2DBTest extends RDFResource2Test {

	public RDFResourceOnEmbedded2DBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
