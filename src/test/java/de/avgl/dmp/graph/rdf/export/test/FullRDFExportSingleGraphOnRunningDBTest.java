package de.avgl.dmp.graph.rdf.export.test;

import de.avgl.dmp.graph.test.Neo4jRunningDBWrapper;

/**
 * @author polowins
 * @author tgaengler
 */
public class FullRDFExportSingleGraphOnRunningDBTest extends FullRDFExportSingleGraphTest {

	public FullRDFExportSingleGraphOnRunningDBTest() {

		super(new Neo4jRunningDBWrapper(), "running");
	}
}
