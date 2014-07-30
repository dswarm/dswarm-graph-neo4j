package org.dswarm.graph.delta.match;

/**
 * Created by tgaengler on 30/07/14.
 */
public class MatchEntity {

	private final long nodeId;

	public MatchEntity(final long nodeIdArg) {

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
		if (!(o instanceof MatchEntity)) {
			return false;
		}

		MatchEntity that = (MatchEntity) o;

		return nodeId == that.nodeId;

	}

	@Override
	public int hashCode() {
		return (int) (nodeId ^ (nodeId >>> 32));
	}
}
