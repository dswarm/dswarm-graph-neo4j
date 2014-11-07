/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.utils;

import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;

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
