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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Optional;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.read.RelationshipHandler;
import org.dswarm.graph.tx.TransactionHandler;
import org.dswarm.graph.versioning.Range;
import org.dswarm.graph.versioning.VersioningStatics;
import org.dswarm.graph.versioning.utils.GraphVersionUtils;

/**
 * @author tgaengler
 */
public abstract class PropertyGraphGDMReader implements GDMReader {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyGraphGDMReader.class);

	protected final NodeHandler         nodeHandler;
	protected final NodeHandler         startNodeHandler;
	protected final RelationshipHandler relationshipHandler;

	protected final String  dataModelUri;
	protected       Integer version;

	protected final GraphDatabaseService database;
	protected final NamespaceIndex       namespaceIndex;

	protected Resource currentResource;
	protected final Map<Long, Statement> currentResourceStatements = new TreeMap<>();

	protected final TransactionHandler tx;

	protected final String type;

	public PropertyGraphGDMReader(final String dataModelUriArg, final Optional<Integer> optionalVersionArg, final GraphDatabaseService databaseArg,
			final
			TransactionHandler txArg,
			final NamespaceIndex namespaceIndexArg,
			final String typeArg) throws DMPGraphException {

		dataModelUri = dataModelUriArg;
		database = databaseArg;
		tx = txArg;
		namespaceIndex = namespaceIndexArg;
		type = typeArg;

		if (optionalVersionArg.isPresent()) {

			version = optionalVersionArg.get();
		} else {

			tx.ensureRunningTx();

			PropertyGraphGDMReader.LOG.debug("start read {} TX", type);

			try {

				version = GraphVersionUtils.getLatestVersion(dataModelUri, database);
			} catch (final Exception e) {

				final String message = "couldn't retrieve latest version successfully";

				PropertyGraphGDMReader.LOG.error(message, e);
				PropertyGraphGDMReader.LOG.debug("couldn't finish read {} TX successfully", type);

				tx.failTx();

				throw new DMPGraphException(message);
			}
		}

		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	private class CBDNodeHandler implements NodeHandler {

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

						if (Range.range(validFrom, validTo).contains(version)) {

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

	private class CBDStartNodeHandler implements NodeHandler {

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

						if (Range.range(validFrom, validTo).contains(version)) {

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

		private final PropertyGraphGDMReaderHelper propertyGraphGDMReaderHelper = new PropertyGraphGDMReaderHelper(namespaceIndex);

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			// note: we can also optionally check for the "resource property at the relationship (this property will only be
			// written right now for model that came as GDM JSON)
			if (rel.getProperty(GraphStatics.DATA_MODEL_PROPERTY).equals(dataModelUri)) {

				final long statementId = rel.getId();

				// subject

				final Node subjectNode = rel.getStartNode();
				final org.dswarm.graph.json.Node subjectGDMNode = propertyGraphGDMReaderHelper.readSubject(subjectNode);

				// predicate

				final String predicate = rel.getType().name();
				final String fullPredicateURI = namespaceIndex.createFullURI(predicate);
				final Predicate predicateProperty = new Predicate(fullPredicateURI);

				// object

				final Node objectNode = rel.getEndNode();
				final org.dswarm.graph.json.Node objectGDMNode = propertyGraphGDMReaderHelper.readObject(objectNode);

				// qualified properties at relationship (statement)

				final Long uuid = (Long) rel.getProperty(GraphStatics.UUID_PROPERTY, null);
				final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);
				final String confidence = (String) rel.getProperty(GraphStatics.CONFIDENCE_PROPERTY, null);
				final String evidence = (String) rel.getProperty(GraphStatics.EVIDENCE_PROPERTY, null);

				final Statement statement = new Statement(subjectGDMNode, predicateProperty, objectGDMNode);
				statement.setId(statementId);

				if (order != null) {

					statement.setOrder(order);
				}

				if (uuid != null) {

					statement.setUUID(uuid.toString());
				}

				if (confidence != null) {

					statement.setConfidence(confidence);
				}

				if (evidence != null) {

					statement.setEvidence(evidence);
				}

				// index should never be null (when resource was written as GDM JSON)
				final Long index = (Long) rel.getProperty(GraphStatics.INDEX_PROPERTY, null);

				if (index != null) {

					currentResourceStatements.put(index, statement);
				} else {

					// note maybe improve this here (however, this is the case for model that where written from RDF)

					currentResource.addStatement(statement);
				}

				if (!objectGDMNode.getType().equals(org.dswarm.graph.json.NodeType.Literal)) {

					// continue traversal with object node
					nodeHandler.handleNode(rel.getEndNode());
				}
			}
		}
	}
}
