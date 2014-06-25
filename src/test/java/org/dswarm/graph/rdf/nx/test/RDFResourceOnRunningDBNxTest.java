package org.dswarm.graph.rdf.nx.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnRunningDBNxTest extends RDFResourceDBNxTest {

	public RDFResourceOnRunningDBNxTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
