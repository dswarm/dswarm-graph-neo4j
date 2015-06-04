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
import org.dswarm.graph.delta.match.mark.ValueEntityMarker;
import org.dswarm.graph.delta.match.model.GDMValueEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class FirstDegreeExactGDMValueMatcher extends Matcher<ValueEntity> {

	public FirstDegreeExactGDMValueMatcher(final Optional<? extends Collection<ValueEntity>> existingValueEntitiesArg,
			final Optional<? extends Collection<ValueEntity>> newValueEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String prefixedExistingResourceURIArg, final String prefixedNewResourceURIArg) throws DMPGraphException {

		super(existingValueEntitiesArg, newValueEntitiesArg, existingResourceDBArg, newResourceDBArg, prefixedExistingResourceURIArg, prefixedNewResourceURIArg,
				new ValueEntityMarker());
	}

	/**
	 * hash with key (predicate), value + value order => matches value entities
	 * 
	 * @param valueEntities
	 * @return
	 */
	@Override protected Map<String, ValueEntity> generateHashes(final Collection<ValueEntity> valueEntities, final GraphDatabaseService resourceD) throws
			DMPGraphException {
		
		final Map<String, ValueEntity> hashedValueEntities = new HashMap<>();


		for(final ValueEntity valueEntity : valueEntities) {

			final int keyHash = valueEntity.getCSEntity().getKey().hashCode();
			final int nodeTypeHash = ((GDMValueEntity) valueEntity).getNodeType().hashCode();

			long valueHash = keyHash;
			valueHash = 31 * valueHash + valueEntity.getValue().hashCode();
			valueHash = 31 * valueHash +  Long.valueOf(valueEntity.getOrder()).hashCode();
			valueHash = 31 * valueHash + nodeTypeHash;

			hashedValueEntities.put(Long.valueOf(valueHash).toString(), valueEntity);
		}

		return hashedValueEntities;
	}
}
