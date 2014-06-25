package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class GDMResourceOnRunningDBTest extends GDMResourceTest {

	public GDMResourceOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
