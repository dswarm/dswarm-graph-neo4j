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
package org.dswarm.graph.gdm.read;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Model;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMModelReader extends PropertyGraphGDMReader implements GDMModelReader {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyGraphGDMModelReader.class);

	private static final String TYPE = "GDM model";

	private final String            recordClassUri;
	private       Optional<Integer> optionalAtMost;

	private Model model;

	public PropertyGraphGDMModelReader(final String recordClassUriArg, final String dataModelUriArg, final Optional<Integer> optionalVersionArg,
			final Optional<Integer> optionalAtMostArg, final GraphDatabaseService databaseArg) throws DMPGraphException {

		super(dataModelUriArg, optionalVersionArg, databaseArg, TYPE);

		recordClassUri = recordClassUriArg;
		optionalAtMost = optionalAtMostArg;
	}

	@Override
	public Model read() throws DMPGraphException {

		ensureTx();

		ResourceIterator<Node> recordNodesIter = null;

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassUri);

			recordNodesIter = database.findNodes(recordClassLabel, GraphStatics.DATA_MODEL_PROPERTY,
					dataModelUri);

			if (recordNodesIter == null) {

				tx.success();

				PropertyGraphGDMModelReader.LOG
						.debug("there are no root nodes for '{}' in data model '{}' finished read {} TX successfully", recordClassLabel, dataModelUri,
								type);

				return null;
			}

			if (!recordNodesIter.hasNext()) {

				recordNodesIter.close();
				tx.success();

				PropertyGraphGDMModelReader.LOG
						.debug("there are no root nodes for '{}' in data model '{}' finished read {} TX successfully", recordClassLabel, dataModelUri,
								type);

				return null;
			}

			model = new Model();

			final Iterator<Node> nodeIterator;

			if (optionalAtMost.isPresent()) {

				nodeIterator = Iterators.limit(recordNodesIter, optionalAtMost.get());
			} else {

				nodeIterator = recordNodesIter;
			}

			while (nodeIterator.hasNext()) {

				final Node recordNode = nodeIterator.next();
				final String resourceUri = (String) recordNode.getProperty(GraphStatics.URI_PROPERTY, null);

				if (resourceUri == null) {

					LOG.debug("there is no resource URI at record node '{}'", recordNode.getId());

					continue;
				}

				currentResource = new Resource(resourceUri);
				startNodeHandler.handleNode(recordNode);

				if (!currentResourceStatements.isEmpty()) {

					// note, this is just an integer number (i.e. NOT long)
					final int mapSize = currentResourceStatements.size();

					long i = 0;

					final Set<Statement> statements = new LinkedHashSet<>();

					while (i < mapSize) {

						i++;

						final Statement statement = currentResourceStatements.get(i);

						statements.add(statement);
					}

					currentResource.setStatements(statements);
				}

				model.addResource(currentResource);

				currentResourceStatements.clear();
			}

			recordNodesIter.close();
			tx.success();

			PropertyGraphGDMModelReader.LOG.debug("finished read {} TX successfully", type);
		} catch (final Exception e) {

			PropertyGraphGDMModelReader.LOG.error("couldn't finished read {} TX successfully", type, e);

			if (recordNodesIter != null) {

				recordNodesIter.close();
			}

			tx.failure();
		} finally {

			PropertyGraphGDMModelReader.LOG.debug("finished read {} TX finally", type);

			tx.close();
		}

		return model;
	}

	@Override
	public long countStatements() {

		return model.size();
	}
}
