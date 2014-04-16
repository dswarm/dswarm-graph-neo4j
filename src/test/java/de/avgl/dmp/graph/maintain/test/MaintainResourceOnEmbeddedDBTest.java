package de.avgl.dmp.graph.maintain.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceOnEmbeddedDBTest extends MaintainResourceTest {

	public MaintainResourceOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
