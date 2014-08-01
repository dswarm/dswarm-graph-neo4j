package org.dswarm.graph.delta.match.model;

import java.util.LinkedList;

/**
 * TODO: impl equals + hashCode
 *
 * Created by tgaengler on 30/07/14.
 */
public class CSEntity extends CompareEntity {


	private long entityOrder;

	private LinkedList<KeyEntity> keyEntities;

	private LinkedList<ValueEntity> valueEntities;

	private String key;

	private boolean keyInitialized = false;

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

		if(!keyInitialized) {

			if(keyEntities == null || keyEntities.isEmpty()) {

				keyInitialized = true;

				return null;
			}

			final StringBuilder sb = new StringBuilder();

			for(final KeyEntity keyEntity : keyEntities) {

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
	}


}
