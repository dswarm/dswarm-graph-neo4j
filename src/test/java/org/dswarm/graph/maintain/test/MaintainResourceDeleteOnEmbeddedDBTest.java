package org.dswarm.graph.maintain.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceDeleteOnEmbeddedDBTest extends MaintainResourceDeleteTest {

	public MaintainResourceDeleteOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
