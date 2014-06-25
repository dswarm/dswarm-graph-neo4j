package org.dswarm.graph.rdf.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

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
