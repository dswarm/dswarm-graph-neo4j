package org.dswarm.graph.maintain.test;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.ClientResponse;

import org.dswarm.graph.rdf.export.test.FullRDFExportTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author phorn
 * @author tgaengler
 */
public abstract class MaintainResourceDeleteTest extends FullRDFExportTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(MaintainResourceDeleteTest.class);

	public MaintainResourceDeleteTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);
	}

	@Test
	public void testDelete() throws Exception {

		LOG.debug("start delete test for maintain resource at " + dbType + " DB");

		final String provenanceURI1 = "http://data.slub-dresden.de/resources/2";
		final String provenanceURI2 = "http://data.slub-dresden.de/resources/3";

		writeRDFToDBInternal(provenanceURI1);
		writeRDFToDBInternal(provenanceURI2);

		final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

		System.out.println("response = " + response);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body shouldn't be null", body);

		Assert.assertEquals("{\"deleted\":10404}", body);

		LOG.debug("finished delete test for maintain resource at " + dbType + " DB");
	}
}
