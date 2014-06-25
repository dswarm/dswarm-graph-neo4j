package org.dswarm.graph.maintain.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceOnRunningDBTest extends MaintainResourceTest {

	public MaintainResourceOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
