package org.dswarm.graph.rdf.export;

import java.util.Map;
import java.util.Map.Entry;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.model.GraphStatics;

public class RDFExporterByProvenance extends RDFExporterBase {

	private static final Logger	LOG				= LoggerFactory.getLogger(RDFExporterByProvenance.class);

	public static final int		CYPHER_LIMIT	= 1000;

	private final String		provenanceURI;

	public RDFExporterByProvenance(final GraphDatabaseService databaseArg, final String provenanceURIArg) {
		super(databaseArg);
		provenanceURI = provenanceURIArg;

	}

	/**
	 * export a data model identified by provenanceURI
	 * 
	 * @return a data model identified by provenanceURI
	 */
	@Override
	public Dataset export() {

		RDFExporterByProvenance.LOG.debug("start exporting data for provenanceURI \"" + provenanceURI + "\"");

		final Transaction tx = database.beginTx();

		try {
			// SR TODO: Keep the ExecutionEngine around, don’t create a new one for each query! (see
			// http://docs.neo4j.org/chunked/milestone/tutorials-cypher-java.html)
			// ...on the other hand we won't need the export very often
			final ExecutionEngine engine = new ExecutionEngine(database);

			dataset = DatasetFactory.createMem();

			boolean requestResults = true;
			long start = 0;

			while (requestResults) {

				final ExecutionResult result = engine.execute("MATCH (n)-[r]->(m) WHERE r." + GraphStatics.PROVENANCE_PROPERTY + " = \""
						+ provenanceURI + "\" RETURN DISTINCT r ORDER BY id(r) SKIP " + start + " LIMIT " + RDFExporterByProvenance.CYPHER_LIMIT);

				start += RDFExporterByProvenance.CYPHER_LIMIT;
				requestResults = false;

				// please note that the Jena model implementation has its size limits (~1 mio statements (?) -> so one graph (of
				// one data resource) need to keep this size in mind)
				if (RDFExporterBase.JENA_MODEL_WARNING_SIZE == start) {
					RDFExporterByProvenance.LOG.warn("reached " + RDFExporterBase.JENA_MODEL_WARNING_SIZE
							+ " statements. This is approximately the jena model implementation size limit.");
				}

				// activate for debug
				// StringBuilder rows = new StringBuilder("\n\n");

				for (final Map<String, Object> row : result) {
					for (final Entry<String, Object> column : row.entrySet()) {

						final Relationship relationship = (Relationship) column.getValue();

						// rows.append(column.getKey()).append(": ");
						// rows.append(relationship).append(": ");
						// rows.append("has provenanceURI \"").append(relationship.getProperty("__PROVENANCE__")).append("\", ");

						// SR TODO to check: do we need to do all the stuff the RelationshipHandler does?
						relationshipHandler.handleRelationship(relationship);

						requestResults = true;

						// for (Node node : relationship.getNodes()) {
						// rows.append("NodeID ").append(node.getId()).append(" (is in relationships [");

						// for (Relationship relation : node.getRelationships()) {
						// rows.append(relation.getId()).append(", ");
						// }

						// rows.append("]), ");
						// }

						// rows.append(";\n");

					}
					// rows.append("\n");
				}
				// rows.append("\n");
				// LOG.debug(rows.toString());
			}

		} catch (final Exception e) {

			RDFExporterByProvenance.LOG.error("couldn't finish read RDF TX successfully", e);

			tx.failure();
			tx.close();
		} finally {

			RDFExporterByProvenance.LOG.debug("finished read RDF TX finally");

			tx.success();
			tx.close();
		}

		return dataset;
	}

}
