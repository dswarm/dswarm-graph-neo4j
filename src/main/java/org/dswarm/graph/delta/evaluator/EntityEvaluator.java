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
package org.dswarm.graph.delta.evaluator;

import java.util.LinkedList;

import org.dswarm.graph.delta.Attribute;
import org.dswarm.graph.delta.util.PathPrinter;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.kernel.Traversal;

/**
 * Created by tgaengler on 30/07/14.
 */
public class EntityEvaluator implements Evaluator {

	final LinkedList<Attribute>	relativeAttributePathAttributes;
	final int relativeAttributePathSizeAttributePathSize;
	int currentHierarchy = 1;

	public EntityEvaluator(final LinkedList<Attribute> relativeAttributePathAttributesArg) {

		relativeAttributePathAttributes = relativeAttributePathAttributesArg;
		relativeAttributePathSizeAttributePathSize = relativeAttributePathAttributes.size();

	}

	@Override
	public Evaluation evaluate(final Path path) {

		if (path.length() > relativeAttributePathSizeAttributePathSize) {

			return Evaluation.EXCLUDE_AND_PRUNE;
		}

		if (path.length() > currentHierarchy) {

			currentHierarchy++;
		}

		if (path.length() < currentHierarchy) {

			return Evaluation.EXCLUDE_AND_CONTINUE;
		}

		final Relationship lastRelationship = path.lastRelationship();
		final String attributeURI = relativeAttributePathAttributes.get(currentHierarchy - 1).getUri();
		lastRelationship.getType().name();
		Traversal.pathToString(path, new PathPrinter());

		if (!lastRelationship.isType(DynamicRelationshipType.withName(attributeURI))) {

			return Evaluation.EXCLUDE_AND_PRUNE;
		}

		if (currentHierarchy < relativeAttributePathSizeAttributePathSize) {

			return Evaluation.EXCLUDE_AND_CONTINUE;
		}

		return Evaluation.INCLUDE_AND_PRUNE;
	}

}
