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

import org.dswarm.graph.delta.DeltaState;

/**
 * @author tgaengler
 */
public class SubGraphEntity extends Entity {

	private final DeltaState	deltaState;
	private final String		predicate;
	private long				order;
	private int hierarchyLevel;
	private final CSEntity		csEntity;

	public SubGraphEntity(final long nodeIdArg, final String predicateArg, final DeltaState deltaStateArg, final CSEntity csEntityArg,
			final long orderArg, final int hierarchyLevelArg) {

		super(nodeIdArg);
		predicate = predicateArg;
		deltaState = deltaStateArg;
		csEntity = csEntityArg;
		order = orderArg;
		hierarchyLevel = hierarchyLevelArg;
	}

	public String getPredicate() {

		return predicate;
	}

	public DeltaState getDeltaState() {

		return deltaState;
	}

	public long getOrder() {

		return order;
	}

	public int getHierarchyLevel() {

		return hierarchyLevel;
	}

	public CSEntity getCSEntity() {

		return csEntity;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof SubGraphEntity)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		final SubGraphEntity that = (SubGraphEntity) o;

		return order == that.order && !(csEntity != null ? !csEntity.equals(that.csEntity) : that.csEntity != null) && deltaState == that.deltaState
				&& !(predicate != null ? !predicate.equals(that.predicate) : that.predicate != null) && hierarchyLevel == that.hierarchyLevel;

	}

	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + (deltaState != null ? deltaState.hashCode() : 0);
		result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
		result = 31 * result + (int) (order ^ (order >>> 32));
		result = 31 * result + hierarchyLevel;
		result = 31 * result + (csEntity != null ? csEntity.hashCode() : 0);

		return result;
	}
}
