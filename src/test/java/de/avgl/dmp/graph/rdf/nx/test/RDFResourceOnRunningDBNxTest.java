package de.avgl.dmp.graph.rdf.nx.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class RDFResourceOnRunningDBNxTest extends RDFResourceDBNxTest {

	public RDFResourceOnRunningDBNxTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
