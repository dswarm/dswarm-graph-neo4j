package de.avgl.dmp.graph.rdf.parse;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import de.avgl.dmp.graph.rdf.GraphStatics;
import de.avgl.dmp.graph.rdf.NodeType;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 * 
 * @author tgaengler
 */
public class Neo4jRDFHandler implements RDFHandler {

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
	private final Index<Node>			resourceTypes;
	private final Map<String, Node>		bnodes;
	private final Index<Relationship>	statements;
	private final Map<Long, String>		nodeResourceMap;

	private Transaction					tx;

	private final String				resourceGraphURI;

	public Neo4jRDFHandler(final GraphDatabaseService database, final String resourceGraphURIArg) {

		this.database = database;
		tx = database.beginTx();
		resources = database.index().forNodes("resources");
		resourceTypes = database.index().forNodes("resource_types");
		bnodes = new HashMap<String, Node>();
		statements = database.index().forRelationships("statements");
		nodeResourceMap = new HashMap<Long, String>();

		resourceGraphURI = resourceGraphURIArg;
	}

	@Override
	public void handleStatement(final Statement st) {

		i++;

		System.out.println("handle statement " + i + ": " + st.toString());

		try {

			final Resource subject = st.getSubject();

			final Property predicate = st.getPredicate();
			final String predicateName = predicate.toString();

			final RDFNode object = st.getObject();

			// Check index for subject
			Node subjectNode = determineNode(subject);

			if (subjectNode == null) {

				subjectNode = database.createNode();

				if (subject.isAnon()) {

					bnodes.put(subject.toString(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				} else {

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subject.toString());
					subjectNode.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
					resources.add(subjectNode, GraphStatics.URI, subject.toString());
				}

				addedNodes++;
			}

			if (object.isLiteral()) {

				literals++;

				final Literal literal = (Literal) object;
				final RDFDatatype type = literal.getDatatype();
				Object value = literal.getValue();
				final Node objectNode = database.createNode();
				objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value);
				objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());

				if (type != null) {

					objectNode.setProperty(GraphStatics.DATATYPE_PROPERTY, type.getURI());
				}

				final String resourceUri = addResourceProperty(subjectNode, subject, objectNode);

				addedNodes++;

				addReleationship(subjectNode, predicateName, objectNode, resourceUri, subject);
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

				if (objectNode == null) {

					objectNode = database.createNode();

					if (object.isAnon()) {

						bnodes.put(object.toString(), objectNode);

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
							resourceUri = addResourceProperty(subjectNode, subject, objectNode);
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeBNode.toString());
							addLabel(objectNode, RDFS.Class.getURI());
						}
					} else {

						objectNode.setProperty(GraphStatics.URI_PROPERTY, object.toString());

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
							addLabel(objectNode, RDFS.Class.getURI());
						}

						resources.add(objectNode, GraphStatics.URI, object.toString());
					}

					addedNodes++;
				}

				addReleationship(subjectNode, predicateName, objectNode, resourceUri, subject);
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 150000 || timeDelta >= 30) { // Commit every 150k operations or every 30 seconds

				tx.success();
				tx.close();
				tx = database.beginTx();

				sinceLastCommit = totalTriples;
				System.out.println(totalTriples + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");
				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			e.printStackTrace();
			tx.close();
			tx = database.beginTx();
		}
	}

	@Override
	public void closeTransaction() {

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

	private Relationship addReleationship(final Node subjectNode, final String predicateName, final Node objectNode, final String resourceUri,
			final Resource subject) {

		final RelationshipType relType = DynamicRelationshipType.withName(predicateName);
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);
		rel.setProperty(GraphStatics.URI_PROPERTY, predicateName);
		rel.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
		statements.add(rel, GraphStatics.ID, Long.valueOf(rel.getId()));

		addedRelationships++;

		addResourceProperty(subjectNode, subject, rel, resourceUri);

		return rel;
	}

	private Node determineNode(final RDFNode resource) {

		final Node node;

		if (resource.isAnon()) {

			node = bnodes.get(resource.toString());

			return node;
		}

		IndexHits<Node> hits = resources.get(GraphStatics.URI, resource.toString());

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

		final Long nodeId = Long.valueOf(subjectNode.getId());

		final String resourceUri;

		if (nodeResourceMap.containsKey(nodeId)) {

			resourceUri = nodeResourceMap.get(nodeId);
		} else {

			resourceUri = subject.toString();
			nodeResourceMap.put(nodeId, resourceUri);
		}

		return resourceUri;
	}
}
