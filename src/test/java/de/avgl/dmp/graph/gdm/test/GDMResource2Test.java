package de.avgl.dmp.graph.gdm.test;

import java.io.IOException;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;

import de.avgl.dmp.graph.test.BasicResourceTest;
import de.avgl.dmp.graph.test.Neo4jDBWrapper;

/**
 * 
 * @author tgaengler
 *
 */
public abstract class GDMResource2Test extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMResource2Test.class);

	public GDMResource2Test(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/gdm", dbTypeArg);
	}

	@Test
	public void writeGDMToTestDB() throws IOException {

		LOG.debug("start write test for GDM resource at " + dbType + " DB");

		writeGDMToTestDBInternal();

		LOG.debug("finished write test for GDM resource at " + dbType + " DB");
	}

	private void writeGDMToTestDBInternal() throws IOException {

		LOG.debug("start writing GDM statements for RDF resource at " + dbType + " DB");

		final URL fileURL = Resources.getResource("test-mabxml.gson");
		final byte[] file = Resources.toByteArray(fileURL);

		// POST the request
		final ClientResponse response = target().path("/put").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, file);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing GDM statements for RDF resource at " + dbType + " DB");
	}
}
