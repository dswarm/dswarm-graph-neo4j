package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

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
