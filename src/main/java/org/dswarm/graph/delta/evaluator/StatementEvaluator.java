package org.dswarm.graph.delta.evaluator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

/**
 * @author tgaengler 
 */
public class StatementEvaluator implements Evaluator {

	final long subjectNodeId;

	public StatementEvaluator(final long subjectNodeIdArg) {

		subjectNodeId = subjectNodeIdArg;
	}

	/**
	 * note: we maybe can also utilise the qualified attribute __LITERAL__ here or the __LITERAL__ node label;
	 *
	 * @param path
	 * @return
	 */
	@Override
	public Evaluation evaluate(final Path path) {

		if(path.length() < 1) {

			return Evaluation.EXCLUDE_AND_CONTINUE;
		}

		if(path.lastRelationship().getStartNode().getId() != subjectNodeId) {

			return Evaluation.EXCLUDE_AND_CONTINUE;
		}

		if(path.endNode().hasRelationship(Direction.OUTGOING)) {

			return Evaluation.EXCLUDE_AND_PRUNE;
		}

		if(path.lastRelationship().hasProperty("MATCHED") && "true".equals((String) path.lastRelationship().getProperty("MATCHED", null))) {

			// only non-matched statements
			return Evaluation.EXCLUDE_AND_PRUNE;
		}

		return Evaluation.INCLUDE_AND_PRUNE;
	}

}
