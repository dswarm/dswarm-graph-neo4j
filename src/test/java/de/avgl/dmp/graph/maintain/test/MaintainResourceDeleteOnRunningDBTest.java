package de.avgl.dmp.graph.maintain.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceDeleteOnRunningDBTest extends MaintainResourceDeleteTest {

	public MaintainResourceDeleteOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
