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

import java.io.OutputStream;
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
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.json.stream.ModelBuilder;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMModelReader extends PropertyGraphGDMReader implements GDMModelReader {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyGraphGDMModelReader.class);

	private static final String TYPE = "GDM model";

	private final String            recordClassUri;
	private       Optional<Integer> optionalAtMost;

	private long size          = 0;
	private long readResources = 0;

	private ModelBuilder modelBuilder;

	public PropertyGraphGDMModelReader(final String recordClassUriArg, final String dataModelUriArg, final Optional<Integer> optionalVersionArg,
			final Optional<Integer> optionalAtMostArg, final GraphDatabaseService databaseArg, final TransactionHandler tx, final NamespaceIndex namespaceIndexArg)
			throws DMPGraphException {

		super(dataModelUriArg, optionalVersionArg, databaseArg, tx, namespaceIndexArg, TYPE);

		recordClassUri = recordClassUriArg;
		optionalAtMost = optionalAtMostArg;
	}

	@Override
	public Optional<ModelBuilder> read(final OutputStream outputStream) throws DMPGraphException {

		readResources = 0;

		tx.ensureRunningTx();

		ResourceIterator<Node> recordNodesIter = null;

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassUri);

			PropertyGraphGDMModelReader.LOG
					.debug("try to read resources for class '{}' in data model '{}' with version '{}'", recordClassLabel, dataModelUri,
							version);

			recordNodesIter = database.findNodes(recordClassLabel, GraphStatics.DATA_MODEL_PROPERTY,
					dataModelUri);

			if (recordNodesIter == null) {

				tx.succeedTx();

				PropertyGraphGDMModelReader.LOG
						.debug("there are no root nodes for '{}' in data model '{}'  with version '{}'; finished read {} TX successfully",
								recordClassLabel, dataModelUri, version,
								type);

				return Optional.absent();
			}

			if (!recordNodesIter.hasNext()) {

				recordNodesIter.close();
				tx.succeedTx();

				PropertyGraphGDMModelReader.LOG
						.debug("there are no root nodes for '{}' in data model '{}'  with version '{}'; finished read {} TX successfully",
								recordClassLabel, dataModelUri, version,
								type);

				return Optional.absent();
			}

			modelBuilder = new ModelBuilder(outputStream);
			size = 0;

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

				final String fullResourceURI = namespaceIndex.createFullURI(resourceUri);

				currentResource = new Resource(fullResourceURI);
				startNodeHandler.handleNode(recordNode);

				if (!currentResourceStatements.isEmpty()) {

					final Set<Statement> statements = new LinkedHashSet<>();
					statements.addAll(currentResourceStatements.values());

					currentResource.setStatements(statements);
				}

				final int resourceStatementSize = currentResource.size();

				if (resourceStatementSize > 0) {

					size += resourceStatementSize;
					modelBuilder.addResource(currentResource);
					readResources++;
				} else {

					LOG.debug("couldn't find any statement for resource '{}' ('{}') in data model '{}' with version '{}'", currentResource.getUri(),
							resourceUri, dataModelUri, version);
				}

				currentResourceStatements.clear();
			}

			recordNodesIter.close();
			tx.succeedTx();

			PropertyGraphGDMModelReader.LOG.debug("finished read {} TX successfully", type);
		} catch (final Exception e) {

			PropertyGraphGDMModelReader.LOG.error("couldn't finished read {} TX successfully", type, e);

			if (recordNodesIter != null) {

				recordNodesIter.close();
			}

			tx.failTx();
		}

		return Optional.of(modelBuilder);
	}

	@Override public long readResources() {

		return readResources;
	}

	@Override
	public long countStatements() {

		return size;
	}
}
