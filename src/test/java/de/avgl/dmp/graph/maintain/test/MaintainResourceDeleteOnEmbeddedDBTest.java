package de.avgl.dmp.graph.maintain.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceDeleteOnEmbeddedDBTest extends MaintainResourceDeleteTest {

	public MaintainResourceDeleteOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
