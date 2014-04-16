package de.avgl.dmp.graph.rdf.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

/**
 * 
 * @author tgaengler
 *
 */
public class RDFResourceOnRunningDBTest extends RDFResourceTest {

	public RDFResourceOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
