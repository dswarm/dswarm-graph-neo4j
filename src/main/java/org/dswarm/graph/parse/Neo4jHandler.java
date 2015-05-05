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
package org.dswarm.graph.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.model.Statement;

/**
 * @author tgaengler
 */
public interface Neo4jHandler {

	Neo4jProcessor getProcessor();

	void handleStatement(final Statement statement) throws DMPGraphException;

	void setResourceUri(final String resourceUri) throws DMPGraphException;

	void setResourceHash(final long resourceHash);

	void resetResourceIndexCounter();

	void closeTransaction() throws DMPGraphException;

	long getCountedStatements();

	int getRelationshipsAdded();

	int getNodesAdded();

	int getCountedLiterals();
}
