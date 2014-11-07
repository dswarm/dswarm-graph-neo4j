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

import org.dswarm.graph.delta.DeltaStatics;
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

		if(path.lastRelationship().hasProperty(DeltaStatics.MATCHED_PROPERTY) && Boolean.TRUE.equals(path.lastRelationship().getProperty(DeltaStatics.MATCHED_PROPERTY, null))) {

			// only non-matched statements
			return Evaluation.EXCLUDE_AND_PRUNE;
		}

		return Evaluation.INCLUDE_AND_PRUNE;
	}

}
