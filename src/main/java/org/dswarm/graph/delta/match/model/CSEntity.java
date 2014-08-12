package org.dswarm.graph.delta.match.model;

import java.util.LinkedList;

/**
 * @author tgaengler
 */
public class CSEntity extends Entity {

	private long					entityOrder;

	private LinkedList<KeyEntity>	keyEntities;

	private LinkedList<ValueEntity>	valueEntities;

	private String					key;

	private boolean					keyInitialized	= false;

	private boolean					hasSubEntities	= false;

	public CSEntity(final long nodeIdArg) {

		super(nodeIdArg);
	}

	public CSEntity(final long nodeIdArg, final long entityOrderArg, final LinkedList<KeyEntity> keyEntitiesArg,
			final LinkedList<ValueEntity> valueEntitiesArg) {

		super(nodeIdArg);
		entityOrder = entityOrderArg;
		keyEntities = keyEntitiesArg;
		valueEntities = valueEntitiesArg;
	}

	public long getEntityOrder() {

		return entityOrder;
	}

	public void setEntityOrder(final long entityOrderArg) {

		entityOrder = entityOrderArg;
	}

	public void setHasSubEntities(final boolean hasSubEntitiesArg) {

		hasSubEntities = hasSubEntitiesArg;
	}

	public boolean hasSubEntities() {

		return hasSubEntities;
	}

	public LinkedList<ValueEntity> getValueEntities() {

		return valueEntities;
	}

	public void setKeyEntities(final LinkedList<KeyEntity> keyEntitiesArg) {

		keyEntities = keyEntitiesArg;
	}

	public void addKeyEntity(final KeyEntity keyEntity) {

		if(keyEntities == null) {

			keyEntities = new LinkedList<>();
		}

		keyEntities.add(keyEntity);
	}

	public LinkedList<KeyEntity> getKeyEntities() {

		return keyEntities;
	}

	public String getKey() {

		if (!keyInitialized) {

			if (keyEntities == null || keyEntities.isEmpty()) {

				keyInitialized = true;

				return null;
			}

			final StringBuilder sb = new StringBuilder();

			for (final KeyEntity keyEntity : keyEntities) {

				sb.append(keyEntity.getValue());
			}

			key = sb.toString();
			keyInitialized = true;
		}

		return key;
	}

	public void setValueEntities(final LinkedList<ValueEntity> valueEntitiesArg) {

		valueEntities = valueEntitiesArg;
	}

	public void addValueEntity(final ValueEntity valueEntity) {

		if(valueEntities == null) {

			valueEntities = new LinkedList<>();
		}

		valueEntities.add(valueEntity);
		valueEntity.setCSEntity(this);
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof CSEntity)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		final CSEntity csEntity = (CSEntity) o;

		return entityOrder == csEntity.entityOrder && hasSubEntities == csEntity.hasSubEntities
				&& !(keyEntities != null ? !keyEntities.equals(csEntity.keyEntities) : csEntity.keyEntities != null)
				&& !(valueEntities != null ? !valueEntities.equals(csEntity.valueEntities) : csEntity.valueEntities != null);

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (int) (entityOrder ^ (entityOrder >>> 32));
		result = 31 * result + (keyEntities != null ? keyEntities.hashCode() : 0);
		result = 31 * result + (valueEntities != null ? valueEntities.hashCode() : 0);
		result = 31 * result + (hasSubEntities ? 1 : 0);

		return result;
	}
}
