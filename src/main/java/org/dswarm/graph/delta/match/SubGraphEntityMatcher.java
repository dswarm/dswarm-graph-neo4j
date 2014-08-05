package org.dswarm.graph.delta.match;

import java.util.Collection;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.SubGraphEntity;

/**
 * @author tgaengler
 */
public abstract class SubGraphEntityMatcher extends Matcher<SubGraphEntity> {

	public SubGraphEntityMatcher(final Collection<SubGraphEntity> existingSubGraphEntitiesArg, final Collection<SubGraphEntity> newSubGraphEntitiesArg) {

		super(existingSubGraphEntitiesArg, newSubGraphEntitiesArg);
	}
}