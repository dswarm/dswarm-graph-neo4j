package org.dswarm.graph.rdf.export;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;

public class GraphRDFExporter extends BaseRDFExporter {

	private static final Logger	LOG	= LoggerFactory.getLogger(GraphRDFExporter.class);

	public GraphRDFExporter(final GraphDatabaseService databaseArg) {
		super(databaseArg);
	}

	@Override
	public Dataset export() throws DMPGraphException {

		try (final Transaction tx = database.beginTx()) {

			/*
			 * // all nodes would also return endnodes without further outgoing relations final Iterable<Node> recordNodes;
			 * GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database); recordNodes =
			 * globalGraphOperations.getAllNodes();
			 */

			final GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database);

			// TODO: maybe slice this a bit, and deliver the whole graph in pieces

			final Iterable<Relationship> relations = globalGraphOperations.getAllRelationships();

			if (relations == null) {

				tx.success();

				return null;
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

				tx.success();
			} catch (final Exception e) {

				final String mesage = "couldn't finish read RDF TX successfully";

			GraphRDFExporter.LOG.error(mesage, e);

				throw new DMPGraphException(mesage);
			}

			return dataset;
		}
}
