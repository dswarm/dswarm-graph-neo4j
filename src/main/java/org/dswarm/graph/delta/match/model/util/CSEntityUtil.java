package org.dswarm.graph.delta.match.model.util;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by tgaengler on 04/08/14.
 */
public final class CSEntityUtil {

	public static Collection<ValueEntity> getValueEntities(final Collection<CSEntity> csEntities) {

		if(csEntities == null || csEntities.isEmpty()) {

			return null;
		}

		final Set<ValueEntity> valueEntities = new HashSet<>();

		for(final CSEntity csEntity : csEntities) {

			valueEntities.addAll(csEntity.getValueEntities());
		}

		return valueEntities;
	}

}
