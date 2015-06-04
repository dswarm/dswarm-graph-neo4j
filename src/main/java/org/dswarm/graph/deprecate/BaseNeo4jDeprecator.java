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
package org.dswarm.graph.deprecate;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.BasicNeo4jProcessor;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.read.RelationshipHandler;
import org.dswarm.graph.utils.GraphUtils;
import org.dswarm.graph.versioning.Range;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
public abstract class BaseNeo4jDeprecator implements RelationshipDeprecator {

	private static final Logger LOG = LoggerFactory.getLogger(BaseNeo4jDeprecator.class);

	protected final NodeHandler         nodeHandler;
	protected final NodeHandler         startNodeHandler;
	protected final RelationshipHandler relationshipHandler;

	protected int i = 0;

	protected VersionHandler versionHandler = null;
	protected int version;
	protected int previousVersion;

	protected final BasicNeo4jProcessor processor;
	protected final boolean             enableVersioning;

	public BaseNeo4jDeprecator(final BasicNeo4jProcessor processorArg, final boolean enableVersioningArg) throws DMPGraphException {

		processor = processorArg;
		enableVersioning = enableVersioningArg;

		init();

		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Neo4jProcessor getProcessor() {

		return processor;
	}

	/**
	 * tx should be running, i.e., no check is done atm
	 *
	 * @param rel
	 * @throws DMPGraphException
	 */
	@Override
	public void deprecateStatement(final Relationship rel) throws DMPGraphException {

		i++;

		///processor.ensureRunningTx();

		try {

			rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, version);
			final Long hashedUUID = (Long) rel.getProperty(GraphStatics.UUID_PROPERTY, null);

			if (hashedUUID == null) {

				LOG.debug("statement/relationship '{}' has no hashed UUID", rel.getId());

				return;
			}

			// remove statement hash from statement hashes index
			final long statementHash = processor.generateStatementHash(rel);
			processor.removeHashFromStatementIndex(statementHash);
		} catch (final Exception e) {

			final String message = "couldn't deprecate statement successfully";

			processor.failTx();

			BaseNeo4jDeprecator.LOG.error(message, e);
			BaseNeo4jDeprecator.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public VersionHandler getVersionHandler() {

		return versionHandler;
	}

	@Override
	public int getRelationshipsDeprecated() {

		return i;
	}

	@Override
	public void closeTransaction() throws DMPGraphException {

		LOG.debug("close write TX finally");

		processor.succeedTx();
		processor.clearMaps();
	}

	protected abstract void init() throws DMPGraphException;

	protected class CBDNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			// => maybe we should find an appropriated cypher query as replacement for this processing
			if (!node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					final Integer validFrom = (Integer) relationship.getProperty(VersioningStatics.VALID_FROM_PROPERTY, null);
					final Integer validTo = (Integer) relationship.getProperty(VersioningStatics.VALID_TO_PROPERTY, null);

					if (validFrom != null && validTo != null) {

						if (Range.range(validFrom, validTo).contains(previousVersion)) {

							relationshipHandler.handleRelationship(relationship);
						}
					} else {

						// TODO: remove this later, when every stmt is versioned
						relationshipHandler.handleRelationship(relationship);
					}
				}
			}
		}
	}

	protected class CBDStartNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// (this is the case for model that came as GDM JSON)
			// node that holds the uri of the resource (record)
			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					final Integer validFrom = (Integer) relationship.getProperty(VersioningStatics.VALID_FROM_PROPERTY, null);
					final Integer validTo = (Integer) relationship.getProperty(VersioningStatics.VALID_TO_PROPERTY, null);

					if (validFrom != null && validTo != null) {

						if (Range.range(validFrom, validTo).contains(previousVersion)) {

							relationshipHandler.handleRelationship(relationship);
						}
					} else {

						// TODO: remove this later, when every stmt is versioned
						relationshipHandler.handleRelationship(relationship);
					}
				}
			}
		}
	}

	private class CBDRelationshipHandler implements RelationshipHandler {

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			deprecateStatement(rel);

			final Node objectNode = rel.getEndNode();
			final NodeType objectNodeType = GraphUtils.determineNodeType(objectNode);

			if(!objectNodeType.equals(NodeType.Literal)) {

				nodeHandler.handleNode(objectNode);
			}
		}
	}
}
