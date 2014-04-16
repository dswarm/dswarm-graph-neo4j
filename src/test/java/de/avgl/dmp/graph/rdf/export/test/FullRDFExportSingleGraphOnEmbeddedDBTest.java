package de.avgl.dmp.graph.rdf.export.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author polowins
 * @author tgaengler
 */
public class FullRDFExportSingleGraphOnEmbeddedDBTest extends FullRDFExportTest {

	public FullRDFExportSingleGraphOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
