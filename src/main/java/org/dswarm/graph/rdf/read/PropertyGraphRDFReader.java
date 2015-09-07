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
package org.dswarm.graph.rdf.read;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.read.NodeHandler;
import org.dswarm.graph.read.RelationshipHandler;
import org.dswarm.graph.tx.TransactionHandler;

/**
 * @author tgaengler
 */
public class PropertyGraphRDFReader implements RDFReader {

	private static final Logger LOG = LoggerFactory.getLogger(PropertyGraphRDFReader.class);

	private final NodeHandler         nodeHandler;
	private final NodeHandler         startNodeHandler;
	private final RelationshipHandler relationshipHandler;

	private final String prefixedRecordClassUri;
	private final String prefixedDataModelUri;

	private final GraphDatabaseService database;
	private final TransactionHandler   tx;
	private final NamespaceIndex       namespaceIndex;

	private Model model;

	public PropertyGraphRDFReader(final String prefixedRcordClassUriArg, final String prefixedDataModelUriArg, final GraphDatabaseService databaseArg,
			final TransactionHandler txArg, final NamespaceIndex namespaceIndexArg) throws DMPGraphException {

		prefixedRecordClassUri = prefixedRcordClassUriArg;
		database = databaseArg;
		tx = txArg;
		namespaceIndex = namespaceIndexArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
		prefixedDataModelUri = prefixedDataModelUriArg;
	}

	@Override
	public Optional<Model> read() throws DMPGraphException {

		tx.ensureRunningTx();

		try {

			LOG.debug("start read RDF TX");

			final Label recordClassLabel = DynamicLabel.label(prefixedRecordClassUri);

			final ResourceIterator<Node> recordNodes = database.findNodes(
					recordClassLabel,
					GraphStatics.DATA_MODEL_PROPERTY,
					prefixedDataModelUri);

			if (recordNodes == null) {

				tx.succeedTx();

				return Optional.absent();
			}

			model = ModelFactory.createDefaultModel();

			while (recordNodes.hasNext()) {

				final Node recordNode = recordNodes.next();
				startNodeHandler.handleNode(recordNode);
			}

			recordNodes.close();

			tx.succeedTx();
		} catch (final Exception e) {

			final String message = "couldn't finish read RDF TX successfully";

			tx.failTx();

			LOG.error(message, e);

			throw new DMPGraphException(message);

		}

		return Optional.of(model);
	}

	@Override
	public long countStatements() {

		return model.size();
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

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDStartNodeHandler implements NodeHandler {

		@Override
		public void handleNode(final Node node) throws DMPGraphException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				for (final Relationship relationship : relationships) {

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDRelationshipHandler implements RelationshipHandler {

		final Map<Long, Resource>   bnodes    = new HashMap<>();
		final Map<String, Resource> resources = new HashMap<>();

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException {

			if (rel.getProperty(GraphStatics.DATA_MODEL_PROPERTY).equals(prefixedDataModelUri)) {

				// TODO: utilise node type for switch

				final String prefixedSubjectURI = (String) rel.getStartNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final Resource subjectResource;

				if (prefixedSubjectURI == null) {

					// prefixedSubjectURI is a bnode

					final long subjectId = rel.getStartNode().getId();
					subjectResource = createResourceFromBNode(subjectId);
				} else {

					final String fullObjectUri = namespaceIndex.createFullURI(prefixedSubjectURI);
					subjectResource = createResourceFromURI(fullObjectUri);
				}

				final String prefixedPredicateURI = rel.getType().name();
				final String predicateURI = namespaceIndex.createFullURI(prefixedPredicateURI);
				//.getProperty(GraphStatics.URI_PROPERTY, null);
				final Property predicate = model.createProperty(predicateURI);

				final String object;

				final String prefixedObjectURI = (String) rel.getEndNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final Resource objectResource;

				// TODO: utilise node type for switch

				if (prefixedObjectURI != null) {

					// object is a resource

					final String fullResourceUri = namespaceIndex.createFullURI(prefixedObjectURI);
					objectResource = createResourceFromURI(fullResourceUri);
				} else {

					// check, whether object is a bnode

					if (!rel.getEndNode().hasProperty(GraphStatics.VALUE_PROPERTY)) {

						// object is a bnode

						final long objectId = rel.getEndNode().getId();

						objectResource = createResourceFromBNode(objectId);

					} else {

						// object is a literal node

						object = (String) rel.getEndNode().getProperty(GraphStatics.VALUE_PROPERTY, null);

						model.add(subjectResource, predicate, object);

						return;
					}
				}

				model.add(subjectResource, predicate, objectResource);

				// continue traversal with object node
				nodeHandler.handleNode(rel.getEndNode());
			}
		}

		private Resource createResourceFromBNode(final long bnodeId) {

			if (!bnodes.containsKey(bnodeId)) {

				bnodes.put(bnodeId, model.createResource());
			}

			return bnodes.get(bnodeId);
		}

		private Resource createResourceFromURI(final String uri) {

			if (!resources.containsKey(uri)) { // TODO: name space prefixing

				resources.put(uri, model.createResource(uri));
			}

			return resources.get(uri);
		}
	}
}
