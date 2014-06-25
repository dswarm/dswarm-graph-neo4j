package org.dswarm.graph.maintain.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author tgaengler
 */
public class MaintainResourceOnEmbeddedDBTest extends MaintainResourceTest {

	public MaintainResourceOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
