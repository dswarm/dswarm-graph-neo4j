package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class GDMResourceOnEmbeddedDBTest extends GDMResourceTest {

	public GDMResourceOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
