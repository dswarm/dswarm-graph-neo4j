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
package org.dswarm.graph;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;

import org.dswarm.graph.index.NamespaceIndex;

/**
 * @author tgaengler
 */
public interface TransactionalNeo4jProcessor extends Neo4jProcessor {

	void ensureRunningTx() throws DMPGraphException;

	void failTx();

	void succeedTx() throws DMPGraphException;

	// TODO: statement uuid is now hashed, i.e., it's a long value
	Optional<Relationship> getRelationshipFromStatementIndex(final String stmtUUID);

	GraphDatabaseService getDatabase();

	NamespaceIndex getNamespaceIndex();
}
