package de.avgl.dmp.graph.rdf.test;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import de.avgl.dmp.graph.test.RunningNeo4jTest;

public class PopulateGraphDBWithSomeRDF  extends FullRDFExportOnRunningDBTest {
	
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
