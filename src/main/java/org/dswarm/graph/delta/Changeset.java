package org.dswarm.graph.delta;

import org.dswarm.graph.json.Statement;

import java.util.Collection;
import java.util.Map;

/**
 * @author tgaengler
 */
public class Changeset {

	private final Map<Long, Collection<Statement>>	additions;
	private final Map<Long, Collection<Statement>>	deletions;
	private final Map<Long, Long>					modifications;
	private final Map<Long, Collection<Statement>>	existingModifiedStatements;
	private final Map<Long, Collection<Statement>>	newModifiedStatements;

	public Changeset(final Map<Long, Collection<Statement>> additions, final Map<Long, Collection<Statement>> deletions,
			final Map<Long, Long> modifications, final Map<Long, Collection<Statement>> existingModifiedStatements,
			final Map<Long, Collection<Statement>> newModifiedStatements) {

		this.additions = additions;
		this.deletions = deletions;
		this.modifications = modifications;
		this.existingModifiedStatements = existingModifiedStatements;
		this.newModifiedStatements = newModifiedStatements;
	}

	public Map<Long, Collection<Statement>> getAdditions() {

		return additions;
	}

	public Map<Long, Collection<Statement>> getDeletions() {

		return deletions;
	}

	public Map<Long, Long> getModifications() {

		return modifications;
	}

	public Map<Long, Collection<Statement>> getExistingModifiedStatements() {

		return existingModifiedStatements;
	}

	public Map<Long, Collection<Statement>> getNewModifiedStatements() {

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
