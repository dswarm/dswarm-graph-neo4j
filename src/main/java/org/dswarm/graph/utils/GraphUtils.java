package org.dswarm.graph.utils;

import javax.ws.rs.core.MediaType;

import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;

import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class GraphUtils {

	private static final Logger		LOG				= LoggerFactory.getLogger(GraphUtils.class);

	// using W3C MIME type standards which partly differ from {@link org.apache.jena.riot.Lang} (see TriG and N3 MIME types in
	// Lang)
	public static final MediaType	TURTLE_TYPE		= new MediaType("text", "turtle");
	public static final String		TURTLE			= "text/turtle";
	public static final MediaType	TRIG_TYPE		= new MediaType("application", "trig");
	public static final String		TRIG			= "application/trig";
	public static final MediaType	N_QUADS_TYPE	= new MediaType("application", "n-quads");
	public static final String		N_QUADS			= "application/n-quads";
	public static final MediaType	RDF_XML_TYPE	= new MediaType("application", "rdf+xml");
	public static final String		RDF_XML			= "application/rdf+xml";
	public static final MediaType	N3_TYPE			= new MediaType("text", "n3");
	public static final String		N3				= "text/n3";

	/**
	 * Hint: There is no LD+JSON parser in jena RIOT, maybe use this one if required:
	 * http://mail-archives.apache.org/mod_mbox/jena-dev/201303.mbox/<513248F7.5010803@apache.org>
	 */
	public static final MediaType	JSONLD_TYPE		= new MediaType("application", "ld+json");

	// public static final String JSONLD = "ld+json";

	public static NodeType determineNodeType(final Node node) throws DMPGraphException {

		final String nodeTypeString = (String) node.getProperty(GraphStatics.NODETYPE_PROPERTY, null);

		if (nodeTypeString == null) {

			final String message = "node type string can't never be null (node id = '" + node.getId() + "')";

			GraphUtils.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final NodeType nodeType = NodeType.getByName(nodeTypeString);

		if (nodeType == null) {

			final String message = "node type can't never be null (node id = '" + node.getId() + "')";

			GraphUtils.LOG.error(message);

			throw new DMPGraphException(message);
		}

		return nodeType;
	}
}
