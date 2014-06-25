package org.dswarm.graph.rdf.nx.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnRunningDBwPROVNxTest extends RDFResourceDBwPROVNxTest {

	public RDFResourceOnRunningDBwPROVNxTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
