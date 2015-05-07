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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;

/**
 * @author tgaengler
 */
public final class GraphUtils {

	private static final Logger LOG = LoggerFactory.getLogger(GraphUtils.class);

	private GraphUtils() { // utils class
	}

	public static NodeType determineNodeType(final Node node) throws DMPGraphException {

		final Iterable<Label> nodeLabels = node.getLabels();

		if (nodeLabels == null) {

			throw new DMPGraphException(String.format("couldn't determine node type; there are no labels at node '%d'", node.getId()));
		}

		for (final Label label : nodeLabels) {

			final String labelName = label.name();

			if (labelName.equals(NodeType.TypeResource.getName())) {

				return NodeType.TypeResource;
			}

			if (labelName.equals(NodeType.TypeBNode.getName())) {

				return NodeType.TypeBNode;
			}

			if (labelName.equals(NodeType.Resource.getName())) {

				return NodeType.Resource;
			}

			if (labelName.equals(NodeType.BNode.getName())) {

				return NodeType.BNode;
			}

			if (labelName.equals(NodeType.Literal.getName())) {

				return NodeType.Literal;
			}
		}

		throw new DMPGraphException(String.format("couldn't determine node type due to missing type labels at node '%d'", node.getId()));
	}
}
