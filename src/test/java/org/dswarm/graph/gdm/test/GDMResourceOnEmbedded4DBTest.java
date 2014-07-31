package org.dswarm.graph.gdm.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;

/**
 *
 * @author tgaengler
 *
 */
public class GDMResourceOnEmbedded4DBTest extends GDMResource4Test {

	public GDMResourceOnEmbedded4DBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
