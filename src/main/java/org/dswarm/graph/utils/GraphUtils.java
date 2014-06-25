package org.dswarm.graph.utils;

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

	private static final Logger	LOG	= LoggerFactory.getLogger(GraphUtils.class);

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
