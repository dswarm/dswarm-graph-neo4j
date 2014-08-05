package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public abstract class CSEntityMatcher extends Matcher<CSEntity> {

	public CSEntityMatcher(final Collection<CSEntity> existingCSEntitiesArg, final Collection<CSEntity> newCSEntitiesArg) {

		super(existingCSEntitiesArg, newCSEntitiesArg);
	}
}
