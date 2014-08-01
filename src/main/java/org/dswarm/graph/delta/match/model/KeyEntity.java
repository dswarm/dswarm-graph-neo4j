package org.dswarm.graph.delta.match.model;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.CompareEntity;

/**
 * TODO: impl equals + hashCode
 *
 * Created by tgaengler on 30/07/14.
 */
public class KeyEntity extends CompareEntity {

	private final String value;
	private CSEntity csEntity;

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
}
