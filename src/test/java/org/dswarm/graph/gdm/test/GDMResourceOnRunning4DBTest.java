package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class GDMResourceOnRunning4DBTest extends GDMResource4Test {

	public GDMResourceOnRunning4DBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
