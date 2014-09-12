package org.dswarm.graph.maintain.test;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.rdf.export.test.RDFExportTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author phorn
 * @author tgaengler
 */
public abstract class MaintainResourceDeleteTest extends RDFExportTest {

	private static final Logger	LOG			= LoggerFactory.getLogger(MaintainResourceDeleteTest.class);
	private static final String	RDF_N3_FILE	= "dmpf_bsp1.n3";

	public MaintainResourceDeleteTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);
	}

	@Test
	public void testDelete() throws Exception {

		MaintainResourceDeleteTest.LOG.debug("start delete test for maintain resource at " + dbType + " DB");

		final String dataModelURI1 = "http://data.slub-dresden.de/resources/2";
		final String dataModelURI2 = "http://data.slub-dresden.de/resources/3";

		writeRDFToDBInternal(dataModelURI1, MaintainResourceDeleteTest.RDF_N3_FILE);
		writeRDFToDBInternal(dataModelURI2, MaintainResourceDeleteTest.RDF_N3_FILE);

		final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

		System.out.println("response = " + response);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body shouldn't be null", body);

		Assert.assertEquals("{\"deleted\":10404}", body);

		MaintainResourceDeleteTest.LOG.debug("finished delete test for maintain resource at " + dbType + " DB");
	}
}
