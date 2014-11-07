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
public class SubGraphLeafEntity extends Entity implements ModificationEntity {

	private final String			value;
	private final SubGraphEntity	subGraphEntity;

	public SubGraphLeafEntity(final long nodeIdArg, final String valueArg, final SubGraphEntity subGraphEntityArg) {

		super(nodeIdArg);
		value = valueArg;
		subGraphEntity = subGraphEntityArg;
	}

	public String getValue() {

		return value;
	}

	public SubGraphEntity getSubGraphEntity() {

		return subGraphEntity;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof SubGraphLeafEntity)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		final SubGraphLeafEntity that = (SubGraphLeafEntity) o;

		return !(subGraphEntity != null ? !subGraphEntity.equals(that.subGraphEntity) : that.subGraphEntity != null)
				&& !(value != null ? !value.equals(that.value) : that.value != null);

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (value != null ? value.hashCode() : 0);
		result = 31 * result + (subGraphEntity != null ? subGraphEntity.hashCode() : 0);

		return result;
	}
}
