package org.dswarm.graph.delta.match;

/**
 * Created by tgaengler on 30/07/14.
 */
public class KeyEntity extends MatchEntity {

	private final String value;

	public KeyEntity(final long nodeIdArg, final String valueArg) {

		super(nodeIdArg);
		value = valueArg;
	}

	public String getValue() {

		return value;
	}
}
