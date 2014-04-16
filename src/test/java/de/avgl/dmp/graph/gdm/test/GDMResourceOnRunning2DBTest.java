package de.avgl.dmp.graph.gdm.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

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
