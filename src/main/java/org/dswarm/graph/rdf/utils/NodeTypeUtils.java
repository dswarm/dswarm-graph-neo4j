package org.dswarm.graph.rdf.utils;

import org.dswarm.graph.NodeType;

import com.google.common.base.Optional;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * @author tgaengler
 */
public final class NodeTypeUtils {

	public static Optional<NodeType> getNodeType(final Optional<RDFNode> optionalNode) {

		return getNodeType(optionalNode, Optional.<Boolean>absent());
	}

	public static Optional<NodeType> getNodeType(final Optional<RDFNode> optionalNode, final Optional<Boolean> optionalIsType) {

		if(!optionalNode.isPresent()) {

			return Optional.absent();
		}

		final NodeType nodeType;
		final RDFNode node = optionalNode.get();

		if(node.isURIResource()) {

			if(optionalIsType.isPresent()) {

				if(Boolean.FALSE.equals(optionalIsType.get())) {

					nodeType = NodeType.Resource;
				} else {

					nodeType = NodeType.TypeResource;
				}
			} else {

				nodeType = NodeType.Resource;
			}
		} else if(node.isAnon()) {

			if(optionalIsType.isPresent()) {

				if(Boolean.FALSE.equals(optionalIsType.get())) {

					nodeType = NodeType.BNode;
				} else {

					nodeType = NodeType.TypeBNode;
				}
			} else {

				nodeType = NodeType.BNode;
			}
		} else if(node.isLiteral()) {

			nodeType = NodeType.Literal;
		} else {

			nodeType = null;
		}

		return Optional.fromNullable(nodeType);
	}
}
