package org.dswarm.graph.delta;

public enum DeltaState {

	ExactMatch("EXACT_MATCH"),
	Modification("MODIFICATION"),
	ADDITION("ADDITION"),
	DELETION("DELETION");

	private final String	name;

	public String getName() {

		return name;
	}

	private DeltaState(final String nameArg) {

		this.name = nameArg;
	}

	public static DeltaState getByName(final String name) {

		for (final DeltaState deltaState : DeltaState.values()) {

			if (deltaState.name.equals(name)) {

				return deltaState;
			}
		}

		throw new IllegalArgumentException(name);
	}

	@Override
	public String toString() {

		return name;
	}
}
