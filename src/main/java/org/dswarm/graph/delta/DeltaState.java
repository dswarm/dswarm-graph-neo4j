/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.delta;

public enum DeltaState {

	ExactMatch("EXACT_MATCH"),
	MODIFICATION("MODIFICATION"),
	ADDITION("ADDITION"),
	DELETION("DELETION");

	private final String name;

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
