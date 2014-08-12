package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class SubGraphLeafEntity extends Entity {

	private final String			value;
	private final SubGraphEntity	subGraphEntity;

	public SubGraphLeafEntity(final long nodeIdArg, final String valueArg, final SubGraphEntity subGraphEntityArg) {

		super(nodeIdArg);
		value = valueArg;
		subGraphEntity = subGraphEntityArg;
	}

	public String getValue() {

		return value;
	}

	public SubGraphEntity getSubGraphEntity() {

		return subGraphEntity;
	}

	@Override
	public boolean equals(final Object o) {

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

		return !(subGraphEntity != null ? !subGraphEntity.equals(that.subGraphEntity) : that.subGraphEntity != null)
				&& !(value != null ? !value.equals(that.value) : that.value != null);

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (value != null ? value.hashCode() : 0);
		result = 31 * result + (subGraphEntity != null ? subGraphEntity.hashCode() : 0);

		return result;
	}
}
