package org.dswarm.graph.delta.match.model;

import org.dswarm.graph.NodeType;

/**
 * note: here we can also take Node, ResourceNode, and LiteralNode from dmp-graph-json
 *
 * @author tgaengler
 */
public class GDMValueEntity extends ValueEntity {

	private final NodeType	nodeType;

	public GDMValueEntity(final long nodeIdArg, final String valueArg, final long orderArg, final NodeType nodeTypeArg) {

		super(nodeIdArg, valueArg, orderArg);

		nodeType = nodeTypeArg;
	}

	public NodeType getNodeType() {

		return nodeType;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof GDMValueEntity)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		final GDMValueEntity that = (GDMValueEntity) o;

		return nodeType == that.nodeType;
	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (nodeType != null ? nodeType.hashCode() : 0);

		return result;
	}
}
