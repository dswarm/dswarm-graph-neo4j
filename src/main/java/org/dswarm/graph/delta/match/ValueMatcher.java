package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public abstract class ValueMatcher extends Matcher<ValueEntity> {

	public ValueMatcher(final Collection<ValueEntity> existingValueEntitiesArg, final Collection<ValueEntity> newValueEntitiesArg) {

		super(existingValueEntitiesArg, newValueEntitiesArg);
	}
}
