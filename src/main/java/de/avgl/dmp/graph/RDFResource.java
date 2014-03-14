package de.avgl.dmp.graph;

import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;

import de.avgl.dmp.graph.rdf.parse.JenaModelParser;
import de.avgl.dmp.graph.rdf.parse.Neo4jRDFHandler;
import de.avgl.dmp.graph.rdf.parse.RDFHandler;
import de.avgl.dmp.graph.rdf.parse.RDFParser;

/**
 * @author tgaengler
 */
@Path("/rdf")
public class RDFResource {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFResource.class);
			//org.neo4j.server.logging.Logger.getLogger(RDFResource.class);

	@GET
	@Path("/ping")
	public String ping() {
		
		LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Consumes("multipart/mixed")
	public Response writeRDF(final MultiPart multiPart, @Context final GraphDatabaseService database) {

		LOG.debug("try to process RDF statements and write them into graph db");

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final Model model = ModelFactory.createDefaultModel();
		model.read(rdfInputStream, null, "N3");
		
		LOG.debug("deserialized RDF statements that were serialised as Turtle or N3");
		
		LOG.debug("try to write RDF statements into graph db");

		final RDFHandler handler = new Neo4jRDFHandler(database, resourceGraphURI);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		LOG.debug("finished writing " + ((Neo4jRDFHandler) handler).getCountedStatements() + " RDF statements into graph db for resource graph URI '"
				+ resourceGraphURI + "'");

		return Response.ok().build();
	}
}
