package org.dswarm.graph.maintain.test;

import org.dswarm.graph.test.BasicResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class MaintainResourceTest extends BasicResourceTest {

	public MaintainResourceTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/maintain", dbTypeArg);
	}
}
