package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class GDMResourceOnRunning2DBTest extends GDMResource2Test {

	public GDMResourceOnRunning2DBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
