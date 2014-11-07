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

import org.dswarm.graph.json.Statement;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;

/**
 * @author tgaengler
 */
public class Changeset {

	private final Map<String, Statement>	additions;
	private final Map<String, Statement>	deletions;
	private final Map<Long, Long>			modifications;
	private final Map<Long, Statement>		existingModifiedStatements;
	private final Map<Long, Statement>		newModifiedStatements;
	private final boolean					hasChanges;

	public Changeset(final Map<String, Statement> additions, final Map<String, Statement> deletions, final Map<Long, Long> modifications,
			final Map<Long, Statement> existingModifiedStatements, final Map<Long, Statement> newModifiedStatements) {

		this.additions = additions;
		this.deletions = deletions;
		this.modifications = modifications;
		this.existingModifiedStatements = existingModifiedStatements;
		this.newModifiedStatements = newModifiedStatements;

		hasChanges = (additions != null && !additions.isEmpty()) || (deletions != null && !deletions.isEmpty())
				|| (modifications != null && !modifications.isEmpty());
	}

	public boolean hasChanges() {

		return hasChanges;
	}

	public Map<String, Statement> getAdditions() {

		return additions;
	}

	public Map<String, Statement> getDeletions() {

		return deletions;
	}

	public Map<Long, Long> getModifications() {

		return modifications;
	}

	public Map<Long, Statement> getExistingModifiedStatements() {

		return existingModifiedStatements;
	}

	public Map<Long, Statement> getNewModifiedStatements() {

		return newModifiedStatements;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {

			return true;
		}
		if (o == null || getClass() != o.getClass()) {

			return false;
		}

		final Changeset changeset = (Changeset) o;

		return !(additions != null ? !additions.equals(changeset.additions) : changeset.additions != null)
				&& !(deletions != null ? !deletions.equals(changeset.deletions) : changeset.deletions != null)
				&& !(existingModifiedStatements != null ? !existingModifiedStatements.equals(changeset.existingModifiedStatements)
						: changeset.existingModifiedStatements != null)
				&& !(modifications != null ? !modifications.equals(changeset.modifications) : changeset.modifications != null)
				&& !(newModifiedStatements != null ? !newModifiedStatements.equals(changeset.newModifiedStatements)
						: changeset.newModifiedStatements != null);

	}

	@Override
	public int hashCode() {

		int result = additions != null ? additions.hashCode() : 0;
		result = 31 * result + (deletions != null ? deletions.hashCode() : 0);
		result = 31 * result + (modifications != null ? modifications.hashCode() : 0);
		result = 31 * result + (existingModifiedStatements != null ? existingModifiedStatements.hashCode() : 0);
		result = 31 * result + (newModifiedStatements != null ? newModifiedStatements.hashCode() : 0);

		return result;
	}
}
