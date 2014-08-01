package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.ValueEntity;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public interface ModificationMatcher {

	Map<ValueEntity, ValueEntity> getModifications();

}
