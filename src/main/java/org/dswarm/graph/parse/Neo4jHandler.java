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
import org.dswarm.graph.model.Statement;

/**
 * @author tgaengler
 */
public interface Neo4jHandler {

	public void handleStatement(final Statement statement) throws DMPGraphException;

	public void setResourceUri(final String resourceUri);

	public void closeTransaction() throws DMPGraphException;

	public long getCountedStatements();

	public int getRelationshipsAdded();

	public int getNodesAdded();

	public int getCountedLiterals();
}
