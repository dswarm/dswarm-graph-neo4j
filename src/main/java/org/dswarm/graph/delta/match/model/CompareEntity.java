package org.dswarm.graph.delta.match.model;

/**
 * Created by tgaengler on 30/07/14.
 */
public class CompareEntity {

	private final long nodeId;

	public CompareEntity(final long nodeIdArg) {

		nodeId = nodeIdArg;
	}

	public long getNodeId() {

		return nodeId;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof CompareEntity)) {
			return false;
		}

		CompareEntity that = (CompareEntity) o;

		return nodeId == that.nodeId;

	}

	@Override
	public int hashCode() {
		return (int) (nodeId ^ (nodeId >>> 32));
	}
}
