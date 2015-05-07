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

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Optional;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;

public class DataModelRDFExporter extends BaseRDFExporter {

	private static final Logger	LOG				= LoggerFactory.getLogger(DataModelRDFExporter.class);

	public static final int CYPHER_LIMIT = 1000;

	private final String dataModelURI;
	private final String prefixedDataModelURI;
	private final TransactionHandler tx;

	public DataModelRDFExporter(final GraphDatabaseService databaseArg, final String dataModelURIArg, final TransactionHandler txArg, final NamespaceIndex namespaceIndex) throws DMPGraphException {
		super(databaseArg, namespaceIndex);
		dataModelURI = dataModelURIArg;
		tx = txArg;
		prefixedDataModelURI = namespaceIndex.createPrefixedURI(dataModelURIArg);
	}

	/**
	 * export a data model identified by dataModelURI
	 *
	 * @return a data model identified by dataModelURI
	 */
	@Override
	public Optional<Dataset> export() throws DMPGraphException {

		DataModelRDFExporter.LOG.debug("start exporting data for dataModelURI \"{}\"", dataModelURI);

		tx.ensureRunningTx();

		try {

			dataset = DatasetFactory.createMem();

			boolean requestResults = true;
			long start = 0;

			while (requestResults) {

				final Result result = database.execute("MATCH (n)-[r]->(m) WHERE r." + GraphStatics.DATA_MODEL_PROPERTY + " = \""
						+ prefixedDataModelURI + "\" RETURN DISTINCT r ORDER BY id(r) SKIP " + start + " LIMIT " + DataModelRDFExporter.CYPHER_LIMIT);

				if(result == null) {

					DataModelRDFExporter.LOG.debug("no results for '{}'", dataModelURI);

					break;
				}

				start += DataModelRDFExporter.CYPHER_LIMIT;
				requestResults = false;

				// please note that the Jena model implementation has its size limits (~1 mio statements (?) -> so one graph (of
				// one data resource) need to keep this size in mind)
				if (BaseRDFExporter.JENA_MODEL_WARNING_SIZE == start) {
					DataModelRDFExporter.LOG.warn("reached " + BaseRDFExporter.JENA_MODEL_WARNING_SIZE
							+ " statements. This is approximately the jena model implementation size limit.");
				}

				// activate for debug
				// StringBuilder rows = new StringBuilder("\n\n");

				while(result.hasNext()) {

					final Map<String, Object> row = result.next();

					for (final Entry<String, Object> column : row.entrySet()) {

						final Relationship relationship = (Relationship) column.getValue();

						// rows.append(column.getKey()).append(": ");
						// rows.append(relationship).append(": ");
						// rows.append("has dataModelURI \"").append(relationship.getProperty("__DATA_MODEL__")).append("\", ");

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

				result.close();
			}

		}  catch (final Exception e) {

			final String mesage = "couldn't finish read RDF TX successfully";

			DataModelRDFExporter.LOG.error(mesage, e);

			throw new DMPGraphException(mesage);
		} finally {

			tx.succeedTx();
		}

		return Optional.fromNullable(dataset);
	}

}
