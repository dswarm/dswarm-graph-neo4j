package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class CompareEntity {

	private final long	nodeId;

	public CompareEntity(final long nodeIdArg) {

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
		if (!(o instanceof CompareEntity)) {
			return false;
		}

		final CompareEntity that = (CompareEntity) o;

		return nodeId == that.nodeId;

	}

	@Override
	public int hashCode() {

		return (int) (nodeId ^ (nodeId >>> 32));
	}
}
