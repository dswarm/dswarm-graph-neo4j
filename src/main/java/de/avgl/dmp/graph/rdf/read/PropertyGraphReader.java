package de.avgl.dmp.graph.rdf.read;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import de.avgl.dmp.graph.rdf.GraphStatics;

/**
 * @author tgaengler
 */
public class PropertyGraphReader implements RDFReader {

	private final NodeHandler			nodeHandler;
	private final NodeHandler			startNodeHandler;
	private final RelationshipHandler	relationshipHandler;

	private final String				recordClassUri;
	private final String				resourceGraphUri;

	private final GraphDatabaseService	database;

	private Model						model;

	public PropertyGraphReader(final String recordClassUriArg, final String resourceGraphUriArg, final GraphDatabaseService databaseArg) {

		recordClassUri = recordClassUriArg;
		resourceGraphUri = resourceGraphUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Model read() {

		final Transaction tx = database.beginTx();

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassUri);

			final ResourceIterable<Node> recordNodes = database.findNodesByLabelAndProperty(recordClassLabel, GraphStatics.PROVENANCE_PROPERTY,
					resourceGraphUri);

			if (recordNodes == null) {

				return null;
			}

			model = ModelFactory.createDefaultModel();

			for (final Node recordNode : recordNodes) {

				startNodeHandler.handleNode(recordNode);
			}
		} catch (final Exception e) {

			// TODO:
		} finally {

			tx.success();
			tx.close();
		}

		return model;
	}

	@Override
	public long countStatements() {

		return model.size();
	}

	private class CBDNodeHandler implements NodeHandler {

		private final Set<Long>	handledRelationships	= new HashSet<Long>();

		@Override
		public void handleNode(final Node node) {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			// => maybe we should find an appropriated cypher query as replacement for this processing
			if (!node.hasProperty(GraphStatics.URI_PROPERTY)) {

				// TODO: how to traverse only in one direction? - currently, we need to check for already processed relationships
				final Iterable<Relationship> relationships = database.traversalDescription().traverse(node).relationships();

				for (final Relationship relationship : relationships) {

					if (handledRelationships.contains(Long.valueOf(relationship.getId()))) {

						continue;
					}

					handledRelationships.add(relationship.getId());

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDStartNodeHandler implements NodeHandler {

		private final Set<Long>	handledRelationships	= new HashSet<Long>();

		@Override
		public void handleNode(final Node node) {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = database.traversalDescription().traverse(node).relationships();

				for (final Relationship relationship : relationships) {

					if (handledRelationships.contains(Long.valueOf(relationship.getId()))) {

						continue;
					}

					handledRelationships.add(relationship.getId());

					relationshipHandler.handleRelationship(relationship);
				}
			}
		}
	}

	private class CBDRelationshipHandler implements RelationshipHandler {

		final Map<String, Resource>	bnodes		= new HashMap<String, Resource>();
		final Map<String, Resource>	resources	= new HashMap<String, Resource>();

		@Override
		public void handleRelationship(final Relationship rel) {

			if (rel.getProperty(GraphStatics.PROVENANCE_PROPERTY).equals(resourceGraphUri)) {

				final String subject = (String) rel.getStartNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final Resource subjectResource;

				if (subject == null) {

					// subject is a bnode

					final String subjectBNode = (String) rel.getStartNode().getProperty(GraphStatics.BNODE_PROPERTY, null);
					subjectResource = createResourceFromBNode(subjectBNode);
				} else {

					subjectResource = createResourceFromURI(subject);
				}

				final String predicate = (String) rel.getProperty(GraphStatics.URI_PROPERTY, null);
				final Property predicateProperty = model.createProperty(predicate);

				final String object;

				final String objectURI = (String) rel.getEndNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final Resource objectResource;

				if (objectURI != null) {

					// object is a resource

					object = objectURI;
					objectResource = createResourceFromURI(object);
				} else {

					// check, whether object is a bnode

					final String objectBNode = (String) rel.getEndNode().getProperty(GraphStatics.BNODE_PROPERTY, null);

					if (objectBNode != null) {

						// object is a bnode

						objectResource = createResourceFromBNode(objectBNode);

					} else {

						// object is a literal node

						object = (String) rel.getEndNode().getProperty(GraphStatics.VALUE_PROPERTY, null);

						model.add(subjectResource, predicateProperty, object);

						return;
					}
				}

				model.add(subjectResource, predicateProperty, objectResource);

				// continue traversal with object node
				nodeHandler.handleNode(rel.getEndNode());
			}
		}

		private Resource createResourceFromBNode(final String bnodeId) {

			if (!bnodes.containsKey(bnodeId)) {

				bnodes.put(bnodeId, model.createResource());
			}

			return bnodes.get(bnodeId);
		}

		private Resource createResourceFromURI(final String uri) {

			if (!resources.containsKey(uri)) {

				resources.put(uri, model.createResource(uri));
			}

			return resources.get(uri);
		}
	}
}
