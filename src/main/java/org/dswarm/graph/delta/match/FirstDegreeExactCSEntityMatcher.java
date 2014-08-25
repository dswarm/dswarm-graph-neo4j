package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.delta.match.mark.CSEntityMarker;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by tgaengler on 01/08/14.
 */
public class FirstDegreeExactCSEntityMatcher extends Matcher<CSEntity> {

	public FirstDegreeExactCSEntityMatcher(final Collection<CSEntity> existingCSEntitiesArg, final Collection<CSEntity> newCSEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String existingResourcURIArg, final String newResourceURIArg) {

		super(existingCSEntitiesArg, newCSEntitiesArg, existingResourceDBArg, newResourceDBArg, existingResourcURIArg, newResourceURIArg, new CSEntityMarker());
	}

	/**
	 * hash with key, value(s) + entity order + value(s) order
	 *
	 * @param csEntities
	 * @return
	 */
	@Override
	protected Map<String, CSEntity> generateHashes(final Collection<CSEntity> csEntities, final GraphDatabaseService resourceD) {

		final Map<String, CSEntity> hashedCSEntities = new HashMap<>();

		for(final CSEntity csEntity : csEntities) {

			final int keyHash = csEntity.getKey().hashCode();
			Long valueHash = null;

			for(final ValueEntity valueEntity : csEntity.getValueEntities()) {

				if(valueHash == null) {

					valueHash = (long) (valueEntity.getValue().hashCode());
					valueHash = 31 * valueHash + Long.valueOf(valueEntity.getOrder()).hashCode();

					continue;
				}

				valueHash = 31 * valueHash + valueEntity.getValue().hashCode();
				valueHash = 31 * valueHash +  Long.valueOf(valueEntity.getOrder()).hashCode();
			}

			long hash = keyHash;
			if(valueHash != null) {
				hash = 31 * hash + valueHash;
			}
			hash = 31 * hash + Long.valueOf(csEntity.getEntityOrder()).hashCode();

			hashedCSEntities.put(Long.valueOf(hash).toString(), csEntity);
		}

		return hashedCSEntities;
	}
}
