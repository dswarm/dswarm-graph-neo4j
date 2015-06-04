/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.match.mark.CSEntityMarker;
import org.dswarm.graph.delta.match.model.CSEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class FirstDegreeExactCSEntityMatcher extends Matcher<CSEntity> {

	private static final Logger LOG = LoggerFactory.getLogger(FirstDegreeExactCSEntityMatcher.class);

	public FirstDegreeExactCSEntityMatcher(final Optional<? extends Collection<CSEntity>> existingCSEntitiesArg,
			final Optional<? extends Collection<CSEntity>> newCSEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String prefixedExistingResourcURIArg, final String prefixedNewResourceURIArg) throws DMPGraphException {

		super(existingCSEntitiesArg, newCSEntitiesArg, existingResourceDBArg, newResourceDBArg, prefixedExistingResourcURIArg, prefixedNewResourceURIArg,
				new CSEntityMarker());

		// TODO: could be removed later
		FirstDegreeExactCSEntityMatcher.LOG.debug("new FirstDegreeExactCSEntityMatcher");
	}

	/**
	 * hash with key, value(s) + entity order + value(s) order
	 *
	 * @param csEntities
	 * @return
	 */
	@Override
	protected Map<String, CSEntity> generateHashes(final Collection<CSEntity> csEntities, final GraphDatabaseService resourceD) throws
			DMPGraphException {

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
