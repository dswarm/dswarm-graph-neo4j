package org.dswarm.graph.rdf.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.GraphDatabaseService;

public class RDFExporterAllData extends RDFExporterBase {

	private static final Logger	LOG	= LoggerFactory.getLogger(RDFExporterAllData.class);
	
	public RDFExporterAllData(GraphDatabaseService databaseArg) {
		super(databaseArg);
	}

	@Override
	public Dataset export() {

		final Transaction tx = database.beginTx();

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

				return null;
			}

			dataset = DatasetFactory.createMem();

			for (final Relationship recordNode : relations) {

				relationshipHandler.handleRelationship(recordNode);

				// please also note that the Jena model implementation has its size limits (~1 mio statements (?) -> so one graph
				// (of
				// one data resource) need to keep this size in mind)
				if (JENA_MODEL_WARNING_SIZE == successfullyProcessedStatements) {
					LOG.warn("reached " + JENA_MODEL_WARNING_SIZE + " statements. This is approximately the jena model implementation size limit.");
				}
			}
		} catch (final Exception e) {

			LOG.error("couldn't finish read RDF TX successfully", e);

			tx.failure();
			tx.close();
		} finally {

			LOG.debug("finished read RDF TX finally");

			tx.success();
			tx.close();
		}

		return dataset;
	}
	
}
