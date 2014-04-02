package de.avgl.dmp.graph.rdf.read;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import de.avgl.dmp.graph.GraphStatics;
import de.avgl.dmp.graph.read.RelationshipHandler;

/**
 * @author polowins
 */
public class PropertyGraphRDFExporter implements RDFReader {

	private static final Logger			LOG	= LoggerFactory.getLogger(PropertyGraphRDFExporter.class);

	private final RelationshipHandler	relationshipHandler;

	private final GraphDatabaseService	database;

	private Model						model;


	public PropertyGraphRDFExporter(final GraphDatabaseService databaseArg) {

		database = databaseArg;
		relationshipHandler = new CBDRelationshipHandler();
	}

	@Override
	public Model read() {
	
		final Transaction tx = database.beginTx();
	
		try {
			
			/*// all nodes would also return endnodes without further outgoing relations
			final Iterable<Node> recordNodes;
			GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database);
			recordNodes = globalGraphOperations.getAllNodes();
			 */
			
			GlobalGraphOperations globalGraphOperations = GlobalGraphOperations.at(database);
	
			final Iterable<Relationship> relations = globalGraphOperations.getAllRelationships();
			
			if (relations == null) {
	
				return null;
			}
	
			model = ModelFactory.createDefaultModel();
	
			for (final Relationship recordNode : relations) {
	
				relationshipHandler.handleRelationship(recordNode);
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

	private class CBDRelationshipHandler implements RelationshipHandler {

		final Map<Long, Resource>	bnodes		= new HashMap<Long, Resource>();
		final Map<String, Resource>	resources	= new HashMap<String, Resource>();

		@Override
		public void handleRelationship(final Relationship rel) {


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

					} else { // object is a literal node
						
						Node endNode = rel.getEndNode();
						object = (String) endNode.getProperty(GraphStatics.VALUE_PROPERTY, null);
						
						if (endNode.hasProperty(GraphStatics.DATATYPE_PROPERTY)) {
							
							String literalType = (String) endNode.getProperty(GraphStatics.DATATYPE_PROPERTY, null);
							
							// object is a typed literal node
							
							Literal typedObject = model.createTypedLiteral(object,literalType);
							
							model.add(subjectResource, predicateProperty, typedObject); 
							return;
							
						} else { 
							
							// object is an untyped literal node
							
							model.add(subjectResource, predicateProperty, object);
							return;
						}

					}
				}

				model.add(subjectResource, predicateProperty, objectResource);
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
