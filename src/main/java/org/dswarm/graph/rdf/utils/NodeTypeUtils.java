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
