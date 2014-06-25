package org.dswarm.graph.maintain.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceDeleteOnRunningDBTest extends MaintainResourceDeleteTest {

	public MaintainResourceDeleteOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
