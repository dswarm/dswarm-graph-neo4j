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
package org.dswarm.graph.rdf.export;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.tx.TransactionHandler;

public class GraphRDFExporter extends BaseRDFExporter {

	private static final Logger	LOG	= LoggerFactory.getLogger(GraphRDFExporter.class);
	private final TransactionHandler tx;

	public GraphRDFExporter(final GraphDatabaseService databaseArg, final TransactionHandler txArg, final NamespaceIndex namespaceIndex) {
		super(databaseArg, namespaceIndex);
		tx = txArg;
	}

	@Override
	public Optional<Dataset> export() throws DMPGraphException {

		tx.ensureRunningTx();

		try {

			/*
			 * // all nodes would also return endnodes without further outgoing relations final Iterable<Node> recordNodes;
			 * GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database); recordNodes =
			 * globalGraphOperations.getAllNodes();
			 */

			final GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database);

			// TODO: maybe slice this a bit, and deliver the whole graph in pieces

			final Iterable<Relationship> relations = globalGraphOperations.getAllRelationships();

			if (relations == null) {

				return Optional.absent();
			}

			dataset = DatasetFactory.createMem();

			for (final Relationship recordNode : relations) {

				relationshipHandler.handleRelationship(recordNode);
			}

			// please also note that the Jena model implementation has its size limits (~1 mio statements (?) -> so one graph
			// (of
			// one data resource) need to keep this size in mind)
			if (BaseRDFExporter.JENA_MODEL_WARNING_SIZE == successfullyProcessedStatements) {
				GraphRDFExporter.LOG.warn("reached " + BaseRDFExporter.JENA_MODEL_WARNING_SIZE
						+ " statements. This is approximately the jena model implementation size limit.");
			}

			} catch (final Exception e) {

				final String mesage = "couldn't finish read RDF TX successfully";

				GraphRDFExporter.LOG.error(mesage, e);

				throw new DMPGraphException(mesage);
			} finally {

			tx.succeedTx();
		}

			return Optional.fromNullable(dataset);
		}
}
