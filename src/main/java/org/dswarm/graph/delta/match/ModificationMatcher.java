package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.match.mark.Marker;
import org.dswarm.graph.delta.match.model.ModificationEntity;
import org.neo4j.graphdb.GraphDatabaseService;

public abstract class ModificationMatcher<ENTITY extends ModificationEntity> extends Matcher<ENTITY> implements ModificationResultSet<ENTITY> {

	private Map<ENTITY, ENTITY>	modifications;

	public ModificationMatcher(final Collection<ENTITY> existingEntitiesArg, final Collection<ENTITY> newEntitiesArg,
			final GraphDatabaseService existingResourceDBArg, final GraphDatabaseService newResourceDBArg, final String existingResourceURIArg,
			final String newResourceURIArg, final Marker<ENTITY> markerArg) {

		super(existingEntitiesArg, newEntitiesArg, existingResourceDBArg, newResourceDBArg, existingResourceURIArg, newResourceURIArg, markerArg);
	}

	@Override
	public void match() {

		getModifications();
		super.match();
		markNonMatchedPaths();
	}

	@Override
	public Map<ENTITY, ENTITY> getModifications() {

		if(!matchesCalculated) {

			modifications = new HashMap<>();
			matches = new HashSet<>();

			for (final Map.Entry<String, ENTITY> existingEntityEntry : existingEntities.entrySet()) {

				if (newEntities.containsKey(existingEntityEntry.getKey())) {

					final ENTITY existingEntity = existingEntityEntry.getValue();
					final ENTITY newEntity = newEntities.get(existingEntityEntry.getKey());

					if (existingEntity.getValue() != null && newEntity.getValue() != null && !existingEntity.getValue()
							.equals(newEntity.getValue())) {

						modifications.put(existingEntity, newEntity);
						matches.add(existingEntityEntry.getKey());
					}
				}
			}

			matchesCalculated = true;
		}

		return modifications;
	}

	@Override
	protected void calculateMatches() {

		// nothing to do
	}

	@Override
	protected void markMatchedPaths() {

		markPaths(getMatches(existingEntities), DeltaState.MODIFICATION, existingResourceDB, existingResourceURI);
		markPaths(getMatches(newEntities), DeltaState.MODIFICATION, newResourceDB, newResourceURI);
	}

	/**
	 * a.k.a. additions + deletions
	 */
	protected void markNonMatchedPaths() {

		markPaths(getNonMatches(existingEntities), DeltaState.DELETION, existingResourceDB, existingResourceURI);
		markPaths(getNonMatches(newEntities), DeltaState.ADDITION, newResourceDB, newResourceURI);
	}

}
