package org.dswarm.graph.rdf.export.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;

/**
 * @author polowins
 * @author tgaengler
 */
public class FullRDFExportMultipleGraphsOnRunningDBTest extends FullRDFExportMultipleGraphsTest {

	public FullRDFExportMultipleGraphsOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
