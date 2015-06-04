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
public final class GraphProcessingStatics {

	public static final String LEAF_IDENTIFIER = "__LEAF__";
	public static final Label LEAF_LABEL = DynamicLabel.label(LEAF_IDENTIFIER);

	public static final String PREFIX_PROPERTY = "prefix";
	public static final String PREFIX_IDENTIFIER = "PREFIX";
	public static final Label PREFIX_LABEL = DynamicLabel.label(PREFIX_IDENTIFIER);

	public static final String RESOURCE_NODE_TYPE = NodeType.Resource.toString();
	public static final String RESOURCE_TYPE_NODE_TYPE = NodeType.TypeResource.toString();
	public static final String LITERAL_NODE_TYPE       = NodeType.Literal.toString();
	public static final String BNODE_NODE_TYPE         = NodeType.BNode.toString();
	public static final String BNODE_TYPE_NODE_TYPE    = NodeType.TypeBNode.toString();

	public static final Label RESOURCE_LABEL = DynamicLabel.label(RESOURCE_NODE_TYPE);
	public static final Label  RESOURCE_TYPE_LABEL     = DynamicLabel.label(RESOURCE_TYPE_NODE_TYPE);
	public static final Label  LITERAL_LABEL           = DynamicLabel.label(LITERAL_NODE_TYPE);
	public static final Label  BNODE_LABEL             = DynamicLabel.label(BNODE_NODE_TYPE);
	public static final Label  BNODE_TYPE_LABEL        = DynamicLabel.label(BNODE_TYPE_NODE_TYPE);
}
