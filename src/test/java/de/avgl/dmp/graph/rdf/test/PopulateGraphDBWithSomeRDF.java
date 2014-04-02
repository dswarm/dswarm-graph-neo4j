package de.avgl.dmp.graph.rdf.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopulateGraphDBWithSomeRDF  extends FullRDFExportMultipleGraphsOnRunningDBTest {
	
	private static final Logger	LOG	= LoggerFactory.getLogger(PopulateGraphDBWithSomeRDF.class);

	public PopulateGraphDBWithSomeRDF() {
		super();
	}

	public static void main (String[] args) {
		
		PopulateGraphDBWithSomeRDF populator = new PopulateGraphDBWithSomeRDF();
		
		LOG.debug("start poppulating the graph with RDF data from file " + TEST_RDF_FILE);

		try {
			populator.writeRDFToRunningDBInternal("http://data.slub-dresden.de/resources/2");
			populator.writeRDFToRunningDBInternal("http://data.slub-dresden.de/resources/3");
			
			LOG.debug("completed poppulating the graph with RDF data from file " + TEST_RDF_FILE);
			
		} catch (IOException e) {
			LOG.error("could not populate the DB correctly. problem processing the file " + TEST_RDF_FILE);
		}
		
	}
}
