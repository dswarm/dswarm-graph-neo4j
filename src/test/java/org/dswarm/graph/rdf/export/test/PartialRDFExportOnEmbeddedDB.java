package org.dswarm.graph.rdf.export.test;

import org.dswarm.graph.test.Neo4jEmbeddedDBWrapper;


public class PartialRDFExportOnEmbeddedDB extends PartialRDFExportTest {

	public PartialRDFExportOnEmbeddedDB() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}