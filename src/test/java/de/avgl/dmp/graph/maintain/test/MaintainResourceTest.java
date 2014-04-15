package de.avgl.dmp.graph.maintain.test;

import de.avgl.dmp.graph.test.BasicResourceTest;
import de.avgl.dmp.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class MaintainResourceTest extends BasicResourceTest {

	public MaintainResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/maintain", dbTypeArg);
	}
}
