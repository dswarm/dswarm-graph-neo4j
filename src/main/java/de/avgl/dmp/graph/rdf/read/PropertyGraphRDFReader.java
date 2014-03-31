package de.avgl.dmp.graph.rdf.read;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import de.avgl.dmp.graph.GraphStatics;
import de.avgl.dmp.graph.read.NodeHandler;
import de.avgl.dmp.graph.read.RelationshipHandler;

/**
 * @author tgaengler
 */
public class PropertyGraphRDFReader implements RDFReader {

	private static final Logger			LOG	= LoggerFactory.getLogger(PropertyGraphRDFReader.class);

	private final NodeHandler			nodeHandler;
	private final NodeHandler			startNodeHandler;
	private final RelationshipHandler	relationshipHandler;

	private final String				recordClassUri;
	private final String				resourceGraphUri;
	
	private boolean 					ignoreProvenance = false;

	private final GraphDatabaseService	database;

	private Model						model;

	public PropertyGraphRDFReader(final String recordClassUriArg, final String resourceGraphUriArg, final GraphDatabaseService databaseArg) {

		recordClassUri = recordClassUriArg;
		resourceGraphUri = resourceGraphUriArg;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	public PropertyGraphRDFReader(final GraphDatabaseService databaseArg) {

		recordClassUri = null;
		resourceGraphUri = null;
		ignoreProvenance = true;
		database = databaseArg;
		nodeHandler = new CBDNodeHandler();
		startNodeHandler = new CBDStartNodeHandler();
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Model read() {

		final Transaction tx = database.beginTx();

		LOG.debug("start read RDF TX");

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

			LOG.error("couldn't finished read RDF TX successfully", e);

			tx.failure();
			tx.close();
		} finally {

			LOG.debug("finished read RDF TX finally");

			tx.success();
			tx.close();
		}

		return model;
	}

	@Override
	public Model readAll() {
	
		final Transaction tx = database.beginTx();
	
		try {
			
			final Iterable<Node> recordNodes;
	
				GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database);
				recordNodes = globalGraphOperations.getAllNodes();
	
			if (recordNodes == null) {
	
				return null;
			}
	
			model = ModelFactory.createDefaultModel();
	
			for (final Node recordNode : recordNodes) {
	
				startNodeHandler.handleNode(recordNode);
			}
		} catch (final Exception e) {
	
			LOG.error("couldn't finish read RDF TX successfully", e);

			tx.failure();
			tx.close();
		} finally {
	
			LOG.debug("finished read RDF TX finally");

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

		@Override
		public void handleNode(final Node node) {

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
		public void handleNode(final Node node) {

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

		final Map<Long, Resource>	bnodes		= new HashMap<Long, Resource>();
		final Map<String, Resource>	resources	= new HashMap<String, Resource>();

		@Override
		public void handleRelationship(final Relationship rel) {

			if (ignoreProvenance || rel.getProperty(GraphStatics.PROVENANCE_PROPERTY).equals(resourceGraphUri)) {

				// TODO: utilise __NODETYPE__ property for switch

				final String subject = (String) rel.getStartNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final Resource subjectResource;

				if (subject == null) {

					// subject is a bnode

					final long subjectId = rel.getStartNode().getId();
					subjectResource = createResourceFromBNode(subjectId);
				} else {

					subjectResource = createResourceFromURI(subject);
				}

				final String predicate = (String) rel.getProperty(GraphStatics.URI_PROPERTY, null);
				final Property predicateProperty = model.createProperty(predicate);

				final String object;

				final String objectURI = (String) rel.getEndNode().getProperty(GraphStatics.URI_PROPERTY, null);

				final Resource objectResource;
				
				// TODO: utilise __NODETYPE__ property for switch

				if (objectURI != null) {

					// object is a resource

					object = objectURI;
					objectResource = createResourceFromURI(object);
				} else {

					// check, whether object is a bnode

					if (!rel.getEndNode().hasProperty(GraphStatics.VALUE_PROPERTY)) {

						// object is a bnode

						final long objectId = rel.getEndNode().getId();

						objectResource = createResourceFromBNode(objectId);

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

		private Resource createResourceFromBNode(final long bnodeId) {

			if (!bnodes.containsKey(Long.valueOf(bnodeId))) {

				bnodes.put(Long.valueOf(bnodeId), model.createResource());
			}

			return bnodes.get(Long.valueOf(bnodeId));
		}

		private Resource createResourceFromURI(final String uri) {

			if (!resources.containsKey(uri)) {

				resources.put(uri, model.createResource(uri));
			}

			return resources.get(uri);
		}
	}
}
