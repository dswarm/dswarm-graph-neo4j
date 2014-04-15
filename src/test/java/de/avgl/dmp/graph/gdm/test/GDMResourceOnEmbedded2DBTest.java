package de.avgl.dmp.graph.gdm.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * 
 * @author tgaengler
 *
 */
public class GDMResourceOnEmbedded2DBTest extends GDMResource2Test {

	public GDMResourceOnEmbedded2DBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
