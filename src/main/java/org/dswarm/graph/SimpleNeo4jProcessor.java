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

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public class SimpleNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(SimpleNeo4jProcessor.class);

	public SimpleNeo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

		super(database);
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
	public Optional<Node> getResourceNodeHits(final String resourceURI) {

		return getNodeFromResourcesIndex(resourceURI);
	}

	@Override protected String putSaltToStatementHash(final String hash) {

		return hash;
	}
}
