package org.dswarm.graph.delta.match.model.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public final class CSEntityUtil {

	public static Optional<? extends Collection<ValueEntity>> getValueEntities(final Optional<? extends Collection<CSEntity>> csEntities) {

		if(!csEntities.isPresent() || csEntities.get().isEmpty()) {

			return Optional.absent();
		}

		final Set<ValueEntity> valueEntities = new HashSet<>();

		for(final CSEntity csEntity : csEntities.get()) {

			valueEntities.addAll(csEntity.getValueEntities());
		}

		return Optional.of(valueEntities);
	}
}
