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
package org.dswarm.graph.rdf.pnx;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.SimpleNeo4jProcessor;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.tx.TransactionHandler;

import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class SimpleRDFNeo4jProcessor extends RDFNeo4jProcessor {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleRDFNeo4jProcessor.class);

	public SimpleRDFNeo4jProcessor(final GraphDatabaseService database, final TransactionHandler tx, final NamespaceIndex namespaceIndex) throws DMPGraphException {

		super(new SimpleNeo4jProcessor(database, tx, namespaceIndex));
	}
}
