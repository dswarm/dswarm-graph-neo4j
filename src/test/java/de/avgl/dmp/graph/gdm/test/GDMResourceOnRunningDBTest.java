package de.avgl.dmp.graph.gdm.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

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
