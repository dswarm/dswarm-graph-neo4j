package de.avgl.dmp.graph.rdf.nx.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnRunningDBwPROVNxTest extends RDFResourceDBwPROVNxTest {

	public RDFResourceOnRunningDBwPROVNxTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
