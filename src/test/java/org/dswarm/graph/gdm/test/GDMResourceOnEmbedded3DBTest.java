package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class GDMResourceOnEmbedded3DBTest extends GDMResource3Test {

	public GDMResourceOnEmbedded3DBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
