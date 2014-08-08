package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class SubGraphLeafEntity extends CompareEntity {

	private final SubGraphEntity	subGraphEntity;

	public SubGraphLeafEntity(final long nodeIdArg, final SubGraphEntity subGraphEntityArg) {

		super(nodeIdArg);
		subGraphEntity = subGraphEntityArg;
	}

	public SubGraphEntity getSubGraphEntity() {

		return subGraphEntity;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof SubGraphLeafEntity)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		final SubGraphLeafEntity that = (SubGraphLeafEntity) o;

		return !(subGraphEntity != null ? !subGraphEntity.equals(that.subGraphEntity) : that.subGraphEntity != null);

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (subGraphEntity != null ? subGraphEntity.hashCode() : 0);

		return result;
	}
}
