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

import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;

/**
 * retrieves the CBD for the given resource URI + data model URI
 *
 * @author tgaengler
 */
public abstract class PropertyGraphGDMResourceReader extends PropertyGraphGDMReader implements GDMResourceReader {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyGraphGDMResourceReader.class);

	public PropertyGraphGDMResourceReader(final String dataModelUriArg, final Optional<Integer> optionalVersionArg,
			final GraphDatabaseService databaseArg, final TransactionHandler tx, final NamespaceIndex namespaceIndexArg, final String type)
			throws DMPGraphException {

		super(dataModelUriArg, optionalVersionArg, databaseArg, tx, namespaceIndexArg, type);
	}

	@Override
	public Resource read() throws DMPGraphException {

		tx.ensureRunningTx();

		try {

			PropertyGraphGDMResourceReader.LOG.debug("start read {} TX", type);

			final Node recordNode = getResourceNode();

			if (recordNode == null) {

				LOG.debug("couldn't find a resource node to start traversal");

				tx.succeedTx();

				PropertyGraphGDMResourceReader.LOG.debug("finished read {} TX successfully", type);

				return null;
			}

			final String resourceUri = (String) recordNode.getProperty(GraphStatics.URI_PROPERTY, null);

			if (resourceUri == null) {

				LOG.debug("there is no resource URI at record node '{}'", recordNode.getId());

				tx.succeedTx();

				PropertyGraphGDMResourceReader.LOG.debug("finished read {} TX successfully", type);

				return null;
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

			tx.succeedTx();

			PropertyGraphGDMResourceReader.LOG.debug("finished read {} TX successfully", type);
		} catch (final Exception e) {

			tx.failTx();

			final String message = String.format("couldn't finished read %s TX successfully", type);

			PropertyGraphGDMResourceReader.LOG.error(message, e);

			throw new DMPGraphException(message);
		}

		return currentResource;
	}

	@Override
	public long countStatements() {

		return currentResource.size();
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @return
	 */
	protected abstract Node getResourceNode() throws DMPGraphException;
}
