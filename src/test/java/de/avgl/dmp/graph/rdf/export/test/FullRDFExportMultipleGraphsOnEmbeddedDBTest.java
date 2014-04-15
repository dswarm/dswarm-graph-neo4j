package de.avgl.dmp.graph.rdf.export.test;

import de.avgl.dmp.graph.test.Neo4jEmbeddedDBWrapper;

/**
 * @author polowins
 * @author tgaengler
 */
public class FullRDFExportMultipleGraphsOnEmbeddedDBTest extends RDFExportMultipleGraphsTest {

	public FullRDFExportMultipleGraphsOnEmbeddedDBTest() {

		super(new Neo4jEmbeddedDBWrapper("/ext"), "embedded");
	}
}
