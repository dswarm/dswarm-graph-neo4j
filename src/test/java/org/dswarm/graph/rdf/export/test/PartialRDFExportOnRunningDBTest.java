package org.dswarm.graph.rdf.export.test;

import org.dswarm.graph.test.Neo4jRunningDBWrapper;


public class PartialRDFExportOnRunningDBTest extends PartialRDFExportTest {

	public PartialRDFExportOnRunningDBTest() {
		super(new Neo4jRunningDBWrapper(), "running");		
	}

}
