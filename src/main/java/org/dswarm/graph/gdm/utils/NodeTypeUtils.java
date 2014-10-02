package org.dswarm.graph.gdm.utils;

import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.Node;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public final class NodeTypeUtils {

	public static Optional<NodeType> getNodeType(final Optional<Node> optionalNode) {

		return getNodeType(optionalNode, Optional.<Boolean>absent());
	}

	public static Optional<NodeType> getNodeType(final Optional<Node> optionalNode, final Optional<Boolean> optionalIsType) {

		if (!optionalNode.isPresent()) {

			return Optional.absent();
		}

		return getNodeTypeByGDMNodeType(Optional.of(optionalNode.get().getType()), optionalIsType);
	}

	public static Optional<NodeType> getNodeTypeByGDMNodeType(final Optional<org.dswarm.graph.json.NodeType> optionalNodeType) {

		return getNodeTypeByGDMNodeType(optionalNodeType, Optional.<Boolean>absent());
	}

	public static Optional<NodeType> getNodeTypeByGDMNodeType(final Optional<org.dswarm.graph.json.NodeType> optionalNodeType, final Optional<Boolean> optionalIsType) {

		if(!optionalNodeType.isPresent()) {

			return Optional.absent();
		}

		final NodeType nodeType;

		switch (optionalNodeType.get()) {

			case Literal:

				nodeType = NodeType.Literal;

				break;
			case Resource:

				if(optionalIsType.isPresent()) {

					if(Boolean.FALSE.equals(optionalIsType.get())) {

						nodeType = NodeType.Resource;
					} else {

						nodeType = NodeType.TypeResource;
					}
				} else {

					nodeType = NodeType.Resource;
				}

				break;
			case BNode:

				if(optionalIsType.isPresent()) {

					if(Boolean.FALSE.equals(optionalIsType.get())) {

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

		return Optional.fromNullable(nodeType);
	}
}
