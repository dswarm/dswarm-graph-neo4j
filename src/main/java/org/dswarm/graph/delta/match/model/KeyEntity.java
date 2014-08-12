package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class KeyEntity extends Entity {

	private final String	value;
	protected CSEntity		csEntity;

	public KeyEntity(final long nodeIdArg, final String valueArg) {

		super(nodeIdArg);
		value = valueArg;
	}

	public String getValue() {

		return value;
	}

	public void setCSEntity(final CSEntity csEntityArg) {

		csEntity = csEntityArg;
	}

	public CSEntity getCSEntity() {

		return csEntity;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {

			return true;
		}
		if (!(o instanceof KeyEntity)) {

			return false;
		}
		if (!super.equals(o)) {

			return false;
		}

		final KeyEntity keyEntity = (KeyEntity) o;

		return !(value != null ? !value.equals(keyEntity.value) : keyEntity.value != null);

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (value != null ? value.hashCode() : 0);

		return result;
	}
}
