package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.ValueEntity;

import java.util.Collection;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public interface ModificationResultSet<VALUEENTITY> {

	/**
	 * returns a map of modified value entities, where the keys are the value entities from the existing resource and the values
	 * are the value entities from the new resource.
	 *
	 * @return
	 */
	Map<VALUEENTITY, VALUEENTITY> getModifications();

}
