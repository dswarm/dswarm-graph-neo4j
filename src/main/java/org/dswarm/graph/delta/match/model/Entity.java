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
package org.dswarm.graph.delta.match.model;

/**
 * @author tgaengler
 */
public class Entity {

	private final Long	nodeId;

	public Entity(final Long nodeIdArg) {

		nodeId = nodeIdArg;
	}

	public Long getNodeId() {

		return nodeId;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {

			return true;
		}

		if (!(o instanceof Entity)) {

			return false;
		}

		final Entity entity = (Entity) o;

		return !(nodeId != null ? !nodeId.equals(entity.nodeId) : entity.nodeId != null);

	}

	@Override
	public int hashCode() {

		return nodeId != null ? nodeId.hashCode() : 0;
	}
}
