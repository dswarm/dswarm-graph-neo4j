package org.dswarm.graph.delta.match;

import java.util.Collection;

import org.dswarm.graph.delta.match.model.SubGraphEntity;
import org.dswarm.graph.delta.match.model.SubGraphLeafEntity;

/**
 * @author tgaengler
 */
public abstract class SubGraphLeafEntityMatcher extends Matcher<SubGraphLeafEntity> {

	public SubGraphLeafEntityMatcher(final Collection<SubGraphLeafEntity> existingSubGraphLeafEntitiesArg,
			final Collection<SubGraphLeafEntity> newSubGraphLeafEntitiesArg) {

		super(existingSubGraphLeafEntitiesArg, newSubGraphLeafEntitiesArg);
	}
}
