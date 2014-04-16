package de.avgl.dmp.graph.rdf.nx.test;

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
public abstract class RDFResourceDBNxTest extends BasicResourceTest {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFResourceDBNxTest.class);

	public RDFResourceDBNxTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, "/rdf", dbTypeArg);
	}

	@Test
	public void writeRDFToDB() throws IOException {

		LOG.debug("start write test for RDF resource at " + dbType + " DB");

		writeRDFToDBInternal();

		LOG.debug("finished write test for RDF resource at " + dbType + " DB");
	}

	private void writeRDFToDBInternal() throws IOException {

		LOG.debug("start writing RDF statements for RDF resource at " + dbType + " DB");

		final URL fileURL = Resources.getResource("dmpf_bsp1.nt");
		final byte[] file = Resources.toByteArray(fileURL);

		// POST the request
		final ClientResponse response = target().path("/putnx").type(MediaType.APPLICATION_OCTET_STREAM).post(ClientResponse.class, file);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		LOG.debug("finished writing RDF statements for RDF resource at " + dbType + " DB");
	}
}
