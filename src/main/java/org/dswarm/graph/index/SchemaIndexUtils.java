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
package org.dswarm.graph.index;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public class SchemaIndexUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SchemaIndexUtils.class);

	public static void createSchemaIndices(final GraphDatabaseService database) throws DMPGraphException {

		getOrCreateSchemaIndex(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.URI_PROPERTY, database);
		getOrCreateSchemaIndex(GraphProcessingStatics.RESOURCE_LABEL, GraphStatics.HASH, database);
		getOrCreateSchemaIndex(GraphProcessingStatics.RESOURCE_TYPE_LABEL, GraphStatics.URI_PROPERTY, database);
		getOrCreateSchemaIndex(GraphProcessingStatics.LITERAL_LABEL, GraphStatics.VALUE_PROPERTY, database);
		getOrCreateSchemaIndex(GraphProcessingStatics.PREFIX_LABEL, GraphStatics.URI_PROPERTY, database);
		getOrCreateSchemaIndex(GraphProcessingStatics.PREFIX_LABEL, GraphProcessingStatics.PREFIX_PROPERTY, database);
	}

	private static void getOrCreateSchemaIndex(final Label label, final String property, final GraphDatabaseService database) throws DMPGraphException {

		final IndexDefinition indexDefinition = SchemaIndexUtils.getOrCreateIndex(label, property, database);

		if (indexDefinition == null) {

			throw new DMPGraphException(
					String.format("something went wrong while index determination/creation for label '%s' and property '%s'", label.name(),
							property));
		}
	}


	public static IndexDefinition getOrCreateIndex(final Label label, final String property, final GraphDatabaseService database) {

		LOG.debug("try to find index for label = '{}' and property = '{}'", label.name(), property);

		boolean notFound = false;

		try (final Transaction tx = database.beginTx()) {

			final Iterable<IndexDefinition> indices = database.schema().getIndexes(label);

			IndexDefinition indexDefinition = null;

			if (indices == null || !indices.iterator().hasNext()) {

				tx.success();
				tx.close();

				notFound = true;
			} else {

				for (final IndexDefinition index : indices) {

					notFound = false;

					indexDefinition = index;

					if (indexDefinition == null) {

						tx.success();
						tx.close();

						notFound = true;

						break;
					}

					final Iterable<String> propIter = indexDefinition.getPropertyKeys();

					boolean propFound = false;

					for(final String prop : propIter) {

						if(prop.equals(property)) {

							propFound = true;

							break;
						}
					}

					if(!propFound) {

						notFound = true;
					}

					if(!notFound) {

						break;
					}
				}
			}

			if (!notFound) {

				LOG.debug("found existing index for label = '{}' and property = '{}'", label.name(), property);

				tx.success();
				tx.close();

				return indexDefinition;
			}
		} catch (final Exception e) {

			LOG.error("sommething went wrong, while index determination for label '{}' and property '{}'", label, property, e);
		}

		if (notFound) {

			return createIndex(label, property, database);
		}

		return null;
	}

	public static IndexDefinition createIndex(final Label label, final String property, final GraphDatabaseService database) {

		LOG.debug("try to create index for label = '{}' and property = '{}'", label.name(), property);

		final IndexDefinition indexDefinition;

		try (final Transaction tx = database.beginTx()) {

			final IndexCreator indexCreator = database.schema().indexFor(label).on(property);
			indexDefinition = indexCreator.create();

			LOG.debug("created index for label = '{}' and property = '{}'", label.name(), property);

			tx.success();
			tx.close();
		} catch (final Exception e) {

			LOG.error("sommething went wrong, while index creation for label '{}' and property '{}'", label, property, e);

			return null;
		}

		return bringIndexOnline(label, property, database, indexDefinition);
	}

	private static IndexDefinition bringIndexOnline(final Label label, final String property, final GraphDatabaseService database,
			final IndexDefinition indexDefinition) {

		try (final Transaction tx = database.beginTx()) {

			LOG.debug("try to bring index online for label = '{}' and property = '{}'", label.name(), property);

			database.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);

			LOG.debug("brought index online for label = '{}' and property = '{}'", label.name(), property);

			tx.success();
			tx.close();

			return indexDefinition;
		} catch (final Exception e) {

			LOG.error("sommething went wrong, while bringing index online for label '{}' and property '{}'", label, property, e);

			return null;
		}
	}
}
