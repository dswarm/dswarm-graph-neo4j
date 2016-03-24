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
package org.dswarm.graph.gdm.utils;

import java.util.Optional;

import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.Node;

/**
 * @author tgaengler
 */
public final class NodeTypeUtils {

	public static Optional<NodeType> getNodeType(final Optional<Node> optionalNode) {

		return getNodeType(optionalNode, Optional.empty());
	}

	public static Optional<NodeType> getNodeType(final Optional<Node> optionalNode, final Optional<Boolean> optionalIsType) {

		if (!optionalNode.isPresent()) {

			return Optional.empty();
		}

		return getNodeTypeByGDMNodeType(Optional.of(optionalNode.get().getType()), optionalIsType);
	}

	public static Optional<NodeType> getNodeTypeByGDMNodeType(final Optional<org.dswarm.graph.json.NodeType> optionalNodeType) {

		return getNodeTypeByGDMNodeType(optionalNodeType, Optional.empty());
	}

	public static Optional<NodeType> getNodeTypeByGDMNodeType(final Optional<org.dswarm.graph.json.NodeType> optionalNodeType, final Optional<Boolean> optionalIsType) {

		if (!optionalNodeType.isPresent()) {

			return Optional.empty();
		}

		final NodeType nodeType;

		switch (optionalNodeType.get()) {

			case Literal:

				nodeType = NodeType.Literal;

				break;
			case Resource:

				if (optionalIsType.isPresent()) {

					if (Boolean.FALSE.equals(optionalIsType.get())) {

						nodeType = NodeType.Resource;
					} else {

						nodeType = NodeType.TypeResource;
					}
				} else {

					nodeType = NodeType.Resource;
				}

				break;
			case BNode:

				if (optionalIsType.isPresent()) {

					if (Boolean.FALSE.equals(optionalIsType.get())) {

						nodeType = NodeType.BNode;
					} else {

						nodeType = NodeType.TypeBNode;
					}
				} else {

					nodeType = NodeType.BNode;
				}

				break;
			default:

				nodeType = null;
		}

		return Optional.ofNullable(nodeType);
	}
}
