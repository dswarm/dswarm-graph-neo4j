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
public class KeyEntity extends Entity implements ModificationEntity {

	private final String	value;
	protected CSEntity		csEntity;

	public KeyEntity(final String valueArg) {

		super(null);
		value = valueArg;
	}

	public KeyEntity(final Long nodeIdArg, final String valueArg) {

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

	@Override
	public boolean equals(final Object o) {

		if (this == o) {

			return true;
		}
		if (!(o instanceof KeyEntity)) {

			return false;
		}
		if (!super.equals(o)) {

			return false;
		}

		final KeyEntity keyEntity = (KeyEntity) o;

		return !(value != null ? !value.equals(keyEntity.value) : keyEntity.value != null);

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (value != null ? value.hashCode() : 0);

		return result;
	}
}
