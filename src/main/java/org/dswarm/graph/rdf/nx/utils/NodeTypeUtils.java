package org.dswarm.graph.rdf.nx.utils;

import org.dswarm.graph.NodeType;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

import com.google.common.base.Optional;
import org.semanticweb.yars.nx.Resource;

/**
 * @author tgaengler
 */
public final class NodeTypeUtils {

	public static Optional<NodeType> getNodeType(final Optional<Node> optionalNode) {

		return getNodeType(optionalNode, Optional.<Boolean>absent());
	}

	public static Optional<NodeType> getNodeType(final Optional<Node> optionalNode, final Optional<Boolean> optionalIsType) {

		if(!optionalNode.isPresent()) {

			return Optional.absent();
		}

		final NodeType nodeType;
		final Node node = optionalNode.get();

		if(node instanceof BNode) {

			if(optionalIsType.isPresent()) {

				if(Boolean.FALSE.equals(optionalIsType.get())) {

					nodeType = NodeType.BNode;
				} else {

					nodeType = NodeType.TypeBNode;
				}
			} else {

				nodeType = NodeType.BNode;
			}


		} else if(node instanceof Literal) {

			nodeType = NodeType.Literal;
		} else if(node instanceof Resource) {

			if(optionalIsType.isPresent()) {

				if(Boolean.FALSE.equals(optionalIsType.get())) {

					nodeType = NodeType.Resource;
				} else {

					nodeType = NodeType.TypeResource;
				}
			} else {

				nodeType = NodeType.Resource;
			}
		} else {

			nodeType = null;
		}

		return Optional.fromNullable(nodeType);
	}
}
