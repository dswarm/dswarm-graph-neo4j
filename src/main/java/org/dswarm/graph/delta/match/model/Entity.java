package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class Entity {

	private final Long	nodeId;

	public Entity(final Long nodeIdArg) {

		nodeId = nodeIdArg;
	}

	public Long getNodeId() {

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

		final Entity entity = (Entity) o;

		return !(nodeId != null ? !nodeId.equals(entity.nodeId) : entity.nodeId != null);

	}

	@Override
	public int hashCode() {

		return nodeId != null ? nodeId.hashCode() : 0;
	}
}
