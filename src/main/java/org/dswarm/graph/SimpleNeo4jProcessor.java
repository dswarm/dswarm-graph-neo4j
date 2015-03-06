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
package org.dswarm.graph;

import org.dswarm.graph.model.GraphStatics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public class SimpleNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(SimpleNeo4jProcessor.class);

	protected final Index<Relationship> statementUUIDs;

	public SimpleNeo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

		super(database);

		ensureRunningTx();

		try {

			statementUUIDs = database.index().forRelationships(GraphIndexStatics.STATEMENT_UUIDS_INDEX_NAME);
		} catch (final Exception e) {

			failTx();

			final String message = "couldn't load indices successfully";

			SimpleNeo4jProcessor.LOG.error(message, e);
			SimpleNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override public Index<Relationship> getStatementUUIDsIndex() {

		return statementUUIDs;
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Node node, final String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			addNodeToResourcesWDataModelIndex(URI, optionalDataModelURI.get(), node);
		}
	}

	@Override
	public void handleObjectDataModel(final Node node, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Node node, String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
			addNodeToResourcesWDataModelIndex(URI, optionalDataModelURI.get(), node);
		}
	}

	@Override
	public void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDs.add(rel, GraphStatics.UUID, statementUUID);
	}

	@Override
	public Optional<Node> getResourceNodeHits(final String resourceURI) {

		return getNodeFromResourcesIndex(resourceURI);
	}
}
