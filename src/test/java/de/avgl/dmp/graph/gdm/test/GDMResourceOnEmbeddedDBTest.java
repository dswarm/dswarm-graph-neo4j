package de.avgl.dmp.graph.gdm.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

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
