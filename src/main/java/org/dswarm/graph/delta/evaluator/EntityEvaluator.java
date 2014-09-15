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
