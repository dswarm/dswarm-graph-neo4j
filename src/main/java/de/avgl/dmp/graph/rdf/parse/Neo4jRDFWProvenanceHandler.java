package de.avgl.dmp.graph.rdf.parse;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.avgl.dmp.graph.DMPGraphException;
import de.avgl.dmp.graph.model.GraphStatics;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.avgl.dmp.graph.NodeType;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 * 
 * @author tgaengler
 */
public class Neo4jRDFWProvenanceHandler implements RDFHandler {

	private static final Logger			LOG					= LoggerFactory.getLogger(Neo4jRDFWProvenanceHandler.class);

	private int							totalTriples		= 0;
	private int							addedNodes			= 0;
	private int							addedLabels			= 0;
	private int							addedRelationships	= 0;
	private int							sinceLastCommit		= 0;
	private int							i					= 0;
	private int							literals			= 0;

	private long						tick				= System.currentTimeMillis();
	private final GraphDatabaseService	database;
	private final Index<Node>			resources;
	private final Index<Node>			resourcesWProvenance;
	private final Index<Node>			resourceTypes;
	private final Index<Node>			values;
	private final Map<String, Node>		bnodes;
	private final Index<Relationship>	statements;
	private final Map<Long, String>		nodeResourceMap;

	private Transaction					tx;

	private final String				resourceGraphURI;

	public Neo4jRDFWProvenanceHandler(final GraphDatabaseService database, final String resourceGraphURIArg) {

		this.database = database;
		tx = database.beginTx();

		LOG.debug("start write TX");

		resources = database.index().forNodes("resources");
		resourcesWProvenance = database.index().forNodes("resources_w_provenance");
		resourceTypes = database.index().forNodes("resource_types");
		values = database.index().forNodes("values");
		bnodes = new HashMap<String, Node>();
		statements = database.index().forRelationships("statements");
		nodeResourceMap = new HashMap<Long, String>();

		resourceGraphURI = resourceGraphURIArg;
	}

	@Override
	public void handleStatement(final Statement st) {

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

		try {

			final Resource subject = st.getSubject();

			final Property predicate = st.getPredicate();
			final String predicateName = predicate.toString();

			final RDFNode object = st.getObject();

			// Check index for subject
			Node subjectNode = determineNode(subject);
			final NodeType subjectNodeType;

			if (subjectNode == null) {

				subjectNode = database.createNode();

				if (subject.isAnon()) {

					bnodes.put(subject.toString(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());

					subjectNodeType = NodeType.BNode;
				} else {

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subject.toString());
					subjectNode.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
					resources.add(subjectNode, GraphStatics.URI, subject.toString());
					resourcesWProvenance.add(subjectNode, GraphStatics.URI_W_PROVENANCE, subject.toString() + resourceGraphURI);

					subjectNodeType = NodeType.Resource;
				}

				addedNodes++;
			} else {

				final String subjectNodeTypeString = (String) subjectNode.getProperty(GraphStatics.NODETYPE_PROPERTY, null);
				subjectNodeType = NodeType.getByName(subjectNodeTypeString);
			}

			if (object.isLiteral()) {

				literals++;

				final Literal literal = (Literal) object;
				final RDFDatatype type = literal.getDatatype();
				Object value = literal.getValue();
				final Node objectNode = database.createNode();
				objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value.toString());
				objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
				values.add(objectNode, GraphStatics.VALUE, value.toString());

				if (type != null) {

					objectNode.setProperty(GraphStatics.DATATYPE_PROPERTY, type.getURI());
				}

				final String resourceUri = addResourceProperty(subjectNode, subject, objectNode);

				addedNodes++;

				addRelationship(subjectNode, predicateName, objectNode, resourceUri, subject, subjectNodeType, NodeType.Literal);
			} else { // must be Resource
						// Make sure object exists

				boolean isType = false;

				// add Label if this is a type entry
				if (predicate.toString().equals(RDF.type.getURI())) {

					addLabel(subjectNode, object.asResource().toString());

					isType = true;
				}

				// Check index for object
				Node objectNode = determineNode(object);
				String resourceUri = null;
				final NodeType objectNodeType;

				if (objectNode == null) {

					objectNode = database.createNode();

					if (object.isAnon()) {

						bnodes.put(object.toString(), objectNode);

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
							resourceUri = addResourceProperty(subjectNode, subject, objectNode);

							objectNodeType = NodeType.BNode;
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeBNode.toString());
							addLabel(objectNode, RDFS.Class.getURI());

							objectNodeType = NodeType.TypeBNode;
						}
					} else {

						objectNode.setProperty(GraphStatics.URI_PROPERTY, object.toString());

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

							objectNodeType = NodeType.Resource;
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
							addLabel(objectNode, RDFS.Class.getURI());

							objectNodeType = NodeType.TypeResource;
						}

						resources.add(objectNode, GraphStatics.URI, object.toString());
						resourcesWProvenance.add(objectNode, GraphStatics.URI_W_PROVENANCE, object.toString() + resourceGraphURI);
					}

					addedNodes++;
				} else {

					final String objectNodeTypeString = (String) objectNode.getProperty(GraphStatics.NODETYPE_PROPERTY, null);
					objectNodeType = NodeType.getByName(objectNodeTypeString);
				}

				addRelationship(subjectNode, predicateName, objectNode, resourceUri, subject, subjectNodeType, objectNodeType);
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 50000 || timeDelta >= 30) { // Commit every 50k operations or every 30 seconds

				tx.success();
				tx.close();
				tx = database.beginTx();

				sinceLastCommit = totalTriples;

				LOG.debug(totalTriples + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");

				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			LOG.error("couldn't finished write TX successfully", e);

			tx.failure();
			tx.close();
			LOG.debug("close a write TX");

			tx = database.beginTx();

			LOG.debug("start another write TX");

		} finally {

			// ???
		}
	}

	@Override
	public void closeTransaction() {

		LOG.debug("close write TX finally");

		tx.success();
		tx.close();
	}

	public int getCountedStatements() {

		return totalTriples;
	}

	public int getNodesAdded() {

		return addedNodes;
	}

	public int getRelationShipsAdded() {

		return addedRelationships;
	}

	public int getCountedLiterals() {

		return literals;
	}

	private void addLabel(final Node node, final String labelString) {

		final Label label = DynamicLabel.label(labelString);
		boolean hit = false;
		final Iterable<Label> labels = node.getLabels();
		final List<Label> labelList = new LinkedList<Label>();

		for (final Label lbl : labels) {

			if (label.equals(lbl)) {

				hit = true;
				break;
			}

			labelList.add(lbl);
		}

		if (!hit) {

			labelList.add(label);
			node.addLabel(label);
			addedLabels++;
		}
	}

	private Relationship addRelationship(final Node subjectNode, final String predicateName, final Node objectNode, final String resourceUri,
			final Resource subject, final NodeType subjectNodeType, final NodeType objectNodeType) throws DMPGraphException {

		final StringBuffer sb = new StringBuffer();

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);
		final String objectIdentifier = getIdentifier(objectNode, objectNodeType);

		final NodeType finalSubjectNodeType = remapNodeType(subjectNodeType);
		final NodeType finalObjectNodeType = remapNodeType(objectNodeType);

		sb.append(finalSubjectNodeType.toString()).append(":").append(subjectIdentifier).append(" ").append(predicateName).append(" ")
				.append(finalObjectNodeType.toString()).append(":").append(objectIdentifier).append(" ").append(resourceGraphURI);
		MessageDigest messageDigest = null;

		try {
			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {

			throw new DMPGraphException("couldn't instantiate hash algo");
		}
		messageDigest.update(sb.toString().getBytes());
		final String hash = new String(messageDigest.digest());

		// System.out.println("hash = '" + hash + "' = '" + sb.toString());

		final Relationship rel;

		IndexHits<Relationship> hits = statements.get(GraphStatics.HASH, hash);

		if (hits == null || !hits.hasNext()) {

			final RelationshipType relType = DynamicRelationshipType.withName(predicateName);
			rel = subjectNode.createRelationshipTo(objectNode, relType);

			// note: this property is not really necessary, since the uri is also the relationship type
			// rel.setProperty(GraphStatics.URI_PROPERTY, predicateName);
			rel.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);

			statements.add(rel, GraphStatics.HASH, hash);

			addedRelationships++;

			addResourceProperty(subjectNode, subject, rel, resourceUri);
		} else {

			rel = hits.next();
		}

		return rel;
	}

	private Node determineNode(final RDFNode resource) {

		final Node node;

		if (resource.isAnon()) {

			node = bnodes.get(resource.toString());

			return node;
		}

		IndexHits<Node> hits = resourcesWProvenance.get(GraphStatics.URI_W_PROVENANCE, resource.toString() + resourceGraphURI);

		if (hits != null && hits.hasNext()) {

			// node exists

			node = hits.next();

			return node;
		}

		return null;
	}

	private String addResourceProperty(final Node subjectNode, final Resource subject, final Node objectNode) {

		final String resourceUri = determineResourceUri(subjectNode, subject);

		if (resourceUri == null) {

			return null;
		}

		// objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, resourceUri);

		return resourceUri;
	}

	private String addResourceProperty(final Node subjectNode, final Resource subject, final Relationship rel, final String resourceUri) {

		final String finalResourceUri;

		if (resourceUri != null) {

			finalResourceUri = resourceUri;
		} else {

			finalResourceUri = determineResourceUri(subjectNode, subject);
		}

		// rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalResourceUri);

		return finalResourceUri;
	}

	private String determineResourceUri(final Node subjectNode, final Resource subject) {

		final Long nodeId = subjectNode.getId();

		final String resourceUri;

		if (nodeResourceMap.containsKey(nodeId)) {

			resourceUri = nodeResourceMap.get(nodeId);
		} else {

			resourceUri = subject.toString();
			nodeResourceMap.put(nodeId, resourceUri);
		}

		return resourceUri;
	}

	private String getIdentifier(final Node node, final NodeType nodeType) {

		final String identifier;

		switch (nodeType) {

			case Resource:
			case TypeResource:

				identifier = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);

				break;
			case BNode:
			case TypeBNode:

				identifier = "" + node.getId();

				break;
			case Literal:

				identifier = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);

				break;
			default:

				identifier = null;

				break;
		}

		return identifier;
	}

	private NodeType remapNodeType(final NodeType nodeType) {

		final NodeType finalNodeType;

		switch(nodeType) {

			case TypeResource:

				finalNodeType = NodeType.Resource;

				break;
			case TypeBNode:

				finalNodeType = NodeType.BNode;

				break;
			default:

				finalNodeType = nodeType;

				break;
		}

		return finalNodeType;
	}
}
