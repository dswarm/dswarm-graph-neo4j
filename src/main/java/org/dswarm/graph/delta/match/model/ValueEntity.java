package org.dswarm.graph.delta.match.model;

import org.dswarm.graph.delta.match.model.KeyEntity;

/**
 * TODO: impl equals + hashCode
 *
 * Created by tgaengler on 30/07/14.
 */
public class ValueEntity extends KeyEntity {

	private final long order;

	public ValueEntity(final long nodeIdArg, final String valueArg, final long orderArg) {

		super(nodeIdArg, valueArg);

		order = orderArg;
	}

	public long getOrder() {

		return order;
	}
}
