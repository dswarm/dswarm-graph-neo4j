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
package org.dswarm.graph;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

/**
 * Holds references for static fields.
 * 
 * @author tgaengler
 */
public interface GraphProcessingStatics {

	String LEAF_IDENTIFIER = "__LEAF__";
	Label LEAF_LABEL = DynamicLabel.label(LEAF_IDENTIFIER);

	String PREFIX_PROPERTY = "prefix";
	String PREFIX_IDENTIFIER = "PREFIX";
	Label PREFIX_LABEL = DynamicLabel.label(PREFIX_IDENTIFIER);

	Label RESOURCE_LABEL      = DynamicLabel.label(NodeType.Resource.toString());
	Label RESOURCE_TYPE_LABEL = DynamicLabel.label(NodeType.TypeResource.toString());
	Label LITERAL_LABEL       = DynamicLabel.label(NodeType.Literal.toString());
	Label BNODE_LABEL       = DynamicLabel.label(NodeType.BNode.toString());
	Label BNODE_TYPE_LABEL       = DynamicLabel.label(NodeType.TypeBNode.toString());
}
