package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class Entity {

	private final long	nodeId;

	public Entity(final long nodeIdArg) {

		nodeId = nodeIdArg;
	}

	public long getNodeId() {

		return nodeId;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof Entity)) {
			return false;
		}

		final Entity that = (Entity) o;

		return nodeId == that.nodeId;

	}

	@Override
	public int hashCode() {

		return (int) (nodeId ^ (nodeId >>> 32));
	}
}
