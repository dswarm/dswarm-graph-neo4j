package org.dswarm.graph.rdf.nx.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnRunningDBwDataModelNxTest extends RDFResourceDBwDataModelNxTest {

	public RDFResourceOnRunningDBwDataModelNxTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
