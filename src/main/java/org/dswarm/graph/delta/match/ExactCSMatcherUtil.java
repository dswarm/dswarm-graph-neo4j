package org.dswarm.graph.delta.match;

import org.dswarm.graph.delta.match.model.CSEntity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by tgaengler on 01/08/14.
 */
public final class ExactCSMatcherUtil {

	public static Collection<CSEntity> getMatches(final Collection<String> matches, final Map<String, CSEntity> csEntityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<CSEntity> csEntities = new ArrayList<>();

		for(final String match : matches) {

			if(csEntityMap.containsKey(match)) {

				csEntities.add(csEntityMap.get(match));
			}
		}

		return csEntities;
	}

	public static Collection<CSEntity> getNonMatches(final Collection<String> matches, final Map<String, CSEntity> csEntityMap) {

		if(matches == null || matches.isEmpty()) {

			return null;
		}

		final List<CSEntity> csEntities = new ArrayList<>();

		for(final Map.Entry<String, CSEntity> csEntityEntry : csEntityMap.entrySet()) {

			if(!matches.contains(csEntityEntry.getKey())) {

				csEntities.add(csEntityEntry.getValue());
			}
		}

		return csEntities;
	}

}
