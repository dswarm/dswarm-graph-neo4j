package org.dswarm.graph.rdf.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

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
