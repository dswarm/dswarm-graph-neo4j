package org.dswarm.graph.delta.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.dswarm.graph.delta.match.mark.ValueEntityMarker;
import org.dswarm.graph.delta.match.model.GDMValueEntity;
import org.dswarm.graph.delta.match.model.ValueEntity;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author tgaengler
 */
public class FirstDegreeExactGDMValueMatcher extends Matcher<ValueEntity> {

	public FirstDegreeExactGDMValueMatcher(final Optional<? extends Collection<ValueEntity>> existingValueEntitiesArg,
			final Optional<? extends Collection<ValueEntity>> newValueEntitiesArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg, final String existingResourceURIArg, final String newResourceURIArg) {

		super(existingValueEntitiesArg, newValueEntitiesArg, existingResourceDBArg, newResourceDBArg, existingResourceURIArg, newResourceURIArg,
				new ValueEntityMarker());
	}

	/**
	 * hash with key (predicate), value + value order => matches value entities
	 * 
	 * @param valueEntities
	 * @return
	 */
	@Override protected Map<String, ValueEntity> generateHashes(final Collection<ValueEntity> valueEntities, final GraphDatabaseService resourceD) {
		
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
