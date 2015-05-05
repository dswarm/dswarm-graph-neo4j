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

import com.github.emboss.siphash.SipHash;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;

/**
 * @author tgaengler
 */
public class SimpleNeo4jProcessor extends BasicNeo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(SimpleNeo4jProcessor.class);

	public SimpleNeo4jProcessor(final GraphDatabaseService database, final TransactionHandler txArg, final NamespaceIndex namespaceIndex) throws DMPGraphException {

		super(database, txArg, namespaceIndex);
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Node node, final String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			final long resourceUriDataModelUriHash = generateResourceHash(URI, optionalDataModelURI);

			addNodeToResourcesWDataModelIndex(URI, resourceUriDataModelUriHash, node);
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

			final long resourceUriDataModelUriHash = generateResourceHash(URI, optionalDataModelURI);

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
			addNodeToResourcesWDataModelIndex(URI, resourceUriDataModelUriHash, node);
		}
	}

	@Override
	public Optional<Node> getResourceNodeHits(final String resourceURI) {

		return getNodeFromResourcesIndex(resourceURI);
	}

	@Override public long generateResourceHash(final String resourceURI, final Optional<String> dataModelURI) {

		final String hashString = resourceURI;

		return SipHash.digest(HashUtils.SPEC_KEY, hashString.getBytes(Charsets.UTF_8));
	}

	@Override protected String putSaltToStatementHash(final String hash) {

		return hash;
	}
}
