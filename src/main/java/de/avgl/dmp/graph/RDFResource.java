package de.avgl.dmp.graph;

import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;

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
	
	@GET
	@Path("/ping")
	public String ping() {
		
		return "pong";
	}

	@POST
	@Consumes("multipart/mixed")
	public Response uploadResource(final MultiPart multiPart, @Context final GraphDatabaseService database) {

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(0).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final String resourceGraphURI = multiPart.getBodyParts().get(1).getEntityAs(String.class);

		final Model model = ModelFactory.createDefaultModel();
		model.read(rdfInputStream, null, "N3");

		final RDFHandler handler = new Neo4jRDFHandler(database, resourceGraphURI);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		return Response.ok().build();
	}
}
