package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class ValueEntity extends KeyEntity {

	private final long	order;

	public ValueEntity(final long nodeIdArg, final String valueArg, final long orderArg) {

		super(nodeIdArg, valueArg);

		order = orderArg;
	}

	public long getOrder() {

		return order;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {

			return true;
		}
		if (!(o instanceof ValueEntity)) {

			return false;
		}
		if (!super.equals(o)) {

			return false;
		}

		final ValueEntity that = (ValueEntity) o;

		return order == that.order;

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (int) (order ^ (order >>> 32));

		return result;
	}
}
