package de.avgl.dmp.graph.rdf.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

/**
 * 
 * @author tgaengler
 *
 */
public class RDFResourceOnRunning2DBTest extends RDFResource2Test {

	public RDFResourceOnRunning2DBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
