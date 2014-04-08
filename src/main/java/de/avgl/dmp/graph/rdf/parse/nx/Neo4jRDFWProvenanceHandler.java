package de.avgl.dmp.graph.rdf.parse.nx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.avgl.dmp.graph.GraphStatics;
import de.avgl.dmp.graph.NodeType;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 * 
 * @author tgaengler
 */
public class Neo4jRDFWProvenanceHandler implements RDFHandler {

	private static final Logger			LOG						= LoggerFactory.getLogger(Neo4jRDFWProvenanceHandler.class);

	private int							totalTriples			= 0;
	private int							addedNodes				= 0;
	private int							addedLabels				= 0;
	private int							addedRelationships		= 0;
	private int							sinceLastCommit			= 0;
	private int							i						= 0;
	private int							literals				= 0;

	private long						tick					= System.currentTimeMillis();
	private final GraphDatabaseService	database;
	// private final Index<Node> resources;
	// private final Index<Node> resourcesWProvenance;
	// private final Index<Node> resourceTypes;
	// private final Index<Node> values;
	private final Map<String, Node>		bnodes;
	private final Index<Relationship>	statements;
	// private final Map<Long, String> nodeResourceMap;

	private Transaction					tx;

	private Label						resourceNodeLabel		= DynamicLabel.label(NodeType.Resource.toString());
	private Label						typeResourceNodeLabel	= DynamicLabel.label(NodeType.TypeResource.toString());
	private Label						literalNodeLabel		= DynamicLabel.label(NodeType.Literal.toString());

	private final String				resourceGraphURI;

	public Neo4jRDFWProvenanceHandler(final GraphDatabaseService database, final String resourceGraphURIArg) {

		this.database = database;
		resourceGraphURI = resourceGraphURIArg;

		getOrCreateIndex(DynamicLabel.label(NodeType.Resource.toString()), new String[] { GraphStatics.URI_PROPERTY });

		// shall we utilise a separate property for this or a combination of two existing properties? - but then, how to
		// effectively utilise this index (instead of the default resources index?
		getOrCreateIndex(DynamicLabel.label(NodeType.Resource.toString()), new String[] { GraphStatics.URI_W_PROVENANCE });

		getOrCreateIndex(DynamicLabel.label(NodeType.TypeResource.toString()), new String[] { GraphStatics.URI_PROPERTY });

		// shall we utilise a schema index for indexing values or utilise a legacy index?
		getOrCreateIndex(DynamicLabel.label(NodeType.Literal.toString()), new String[] { GraphStatics.VALUE_PROPERTY });

		// resources = database.index().forNodes("resources");
		// resourcesWProvenance = database.index().forNodes("resources_w_provenance");
		// resourceTypes = database.index().forNodes("resource_types");
		// values = database.index().forNodes("values");
		bnodes = new HashMap<String, Node>();

		try (Transaction tx = database.beginTx()) {

			statements = database.index().forRelationships("statements");
			tx.success();
		}

		// nodeResourceMap = new HashMap<Long, String>();

		tx = database.beginTx();

		LOG.debug("start write TX");
	}

	@Override
	public void handleStatement(final org.semanticweb.yars.nx.Node[] st) {

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

		try {

			final org.semanticweb.yars.nx.Node subject = st[0];

			final org.semanticweb.yars.nx.Node predicate = st[1];
			final String predicateName = predicate.toString();

			final org.semanticweb.yars.nx.Node object = st[2];

			// Check index for subject
			Node subjectNode = determineNode(subject);

			if (subjectNode == null) {

				if (subject instanceof BNode) {

					subjectNode = database.createNode();

					bnodes.put(subject.toString(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				} else {

					subjectNode = database.createNode(resourceNodeLabel);

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subject.toString());
					subjectNode.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
					// resources.add(subjectNode, GraphStatics.URI, subject.toString());
					// resourcesWProvenance.add(subjectNode, GraphStatics.URI_W_PROVENANCE, subject.toString() +
					// resourceGraphURI);
					subjectNode.setProperty(GraphStatics.URI_W_PROVENANCE_PROPERTY, subject.toString() + " " + resourceGraphURI);
				}

				addedNodes++;
			}

			if (object instanceof Literal) {

				literals++;

				final Literal literal = (Literal) object;
				final Resource type = literal.getDatatype();
				String value = literal.getData();
				final Node objectNode = database.createNode(literalNodeLabel);
				objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value);
				objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
				// values.add(objectNode, GraphStatics.VALUE, value);

				if (type != null) {

					objectNode.setProperty(GraphStatics.DATATYPE_PROPERTY, type.toString());
				}

				// final String resourceUri = addResourceProperty(subjectNode, subject, objectNode);

				addedNodes++;

				addReleationship(subjectNode, predicateName, objectNode, /* resourceUri */null, subject);
			} else { // must be Resource
						// Make sure object exists

				boolean isType = false;

				// add Label if this is a type entry
				if (predicate.toString().equals(RDF.TYPE.toString())) {

					addLabel(subjectNode, object.toString());

					isType = true;
				}

				// Check index for object
				Node objectNode = determineNode(object);
				String resourceUri = null;

				if (objectNode == null) {

					if (object instanceof BNode) {

						objectNode = database.createNode();

						bnodes.put(object.toString(), objectNode);

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
							// resourceUri = addResourceProperty(subjectNode, subject, objectNode);
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeBNode.toString());
							addLabel(objectNode, RDFS.CLASS.toString());
						}

						objectNode.setProperty(GraphStatics.URI_W_PROVENANCE_PROPERTY, object.toString() + " " + resourceGraphURI);
					} else {

						if (!isType) {

							objectNode = database.createNode(resourceNodeLabel);

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

							// note: this might be not correct ... i.e., this makes this resource also data model dependent
							objectNode.setProperty(GraphStatics.URI_W_PROVENANCE_PROPERTY, object.toString() + " " + resourceGraphURI);
						} else {

							objectNode = database.createNode(resourceNodeLabel, typeResourceNodeLabel);

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
							addLabel(objectNode, RDFS.CLASS.toString());

							// note: no URI_W_PROVENANCE property here, to keep types data model independent
						}

						objectNode.setProperty(GraphStatics.URI_PROPERTY, object.toString());

						// resources.add(objectNode, GraphStatics.URI, object.toString());
						// resourcesWProvenance.add(objectNode, GraphStatics.URI_W_PROVENANCE, object.toString() +
						// resourceGraphURI);
					}

					addedNodes++;
				}

				addReleationship(subjectNode, predicateName, objectNode, resourceUri, subject);
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

	private Relationship addReleationship(final Node subjectNode, final String predicateName, final Node objectNode, final String resourceUri,
			final org.semanticweb.yars.nx.Node subject) {

		final RelationshipType relType = DynamicRelationshipType.withName(predicateName);
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);
		rel.setProperty(GraphStatics.URI_PROPERTY, predicateName);
		rel.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
		statements.add(rel, GraphStatics.ID, Long.valueOf(rel.getId()));

		addedRelationships++;

		// addResourceProperty(subjectNode, subject, rel, resourceUri);

		return rel;
	}

	private Node determineNode(final org.semanticweb.yars.nx.Node resource) {

		final Node node;

		if (resource instanceof BNode) {

			node = bnodes.get(resource.toString());

			return node;
		}

		// IndexHits<Node> hits = resources.get(GraphStatics.URI, resource.toString());

		// 1. get by resource uri + provenance uri

		ResourceIterable<Node> hits = database.findNodesByLabelAndProperty(resourceNodeLabel, GraphStatics.URI_W_PROVENANCE_PROPERTY,
				resource.toString() + " " + resourceGraphURI);

		if (hits != null && hits.iterator().hasNext()) {

			// node exists

			node = hits.iterator().next();

			return node;
		}

		// 2. get by resource uri

		ResourceIterable<Node> hits2 = database.findNodesByLabelAndProperty(resourceNodeLabel, GraphStatics.URI_PROPERTY, resource.toString());

		if (hits2 != null && hits2.iterator().hasNext()) {

			// node exists

			node = hits2.iterator().next();

			return node;
		}

		return null;
	}

	// private String addResourceProperty(final Node subjectNode, final org.semanticweb.yars.nx.Node subject, final Node
	// objectNode) {
	//
	// final String resourceUri = determineResourceUri(subjectNode, subject);
	//
	// if (resourceUri == null) {
	//
	// return null;
	// }
	//
	// // objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, resourceUri);
	//
	// return resourceUri;
	// }

	// private String addResourceProperty(final Node subjectNode, final org.semanticweb.yars.nx.Node subject, final Relationship
	// rel,
	// final String resourceUri) {
	//
	// final String finalResourceUri;
	//
	// if (resourceUri != null) {
	//
	// finalResourceUri = resourceUri;
	// } else {
	//
	// finalResourceUri = determineResourceUri(subjectNode, subject);
	// }
	//
	// // rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalResourceUri);
	//
	// return finalResourceUri;
	// }

	// private String determineResourceUri(final Node subjectNode, final org.semanticweb.yars.nx.Node subject) {
	//
	// final Long nodeId = Long.valueOf(subjectNode.getId());
	//
	// final String resourceUri;
	//
	// if (nodeResourceMap.containsKey(nodeId)) {
	//
	// resourceUri = nodeResourceMap.get(nodeId);
	// } else {
	//
	// resourceUri = subject.toString();
	// nodeResourceMap.put(nodeId, resourceUri);
	// }
	//
	// return resourceUri;
	// }

	private IndexDefinition getOrCreateIndex(final Label label, final String[] properties) {

		IndexDefinition indexDefinition = null;

		final StringBuilder sb = new StringBuilder();

		for (final String property : properties) {

			sb.append(property).append(" ");
		}

		final String propertiesString = sb.toString();

		LOG.debug("try to find index for label = '" + label.name() + "' and properties = '" + propertiesString + "'");

		try (Transaction tx = database.beginTx()) {

			Iterable<IndexDefinition> indices = database.schema().getIndexes(label);

			if (indices != null && indices.iterator().hasNext()) {

				Iterator<IndexDefinition> idIter = indices.iterator();

				while (idIter.hasNext()) {

					indexDefinition = idIter.next();

					final Iterable<String> propertyKeys = indexDefinition.getPropertyKeys();

					Set<String> propertyKeySet = new HashSet<String>();

					for (final String propertyKey : propertyKeys) {

						propertyKeySet.add(propertyKey);
					}

					if (properties.length == propertyKeySet.size()) {

						for (final String property : properties) {

							if (!propertyKeySet.contains(property)) {

								indexDefinition = null;

								break;
							}
						}

						if (indexDefinition != null) {

							// found match

							break;
						}
					}
				}
			}

			tx.success();
		}

		if (indexDefinition != null) {

			LOG.debug("found existing index for label = '" + label.name() + "' and properties = '" + propertiesString + "'");

			return indexDefinition;
		}

		return createIndex(label, properties, propertiesString);
	}

	private IndexDefinition createIndex(final Label label, final String[] properties, final String propertiesString) {

		IndexDefinition indexDefinition = null;

		LOG.debug("try to create index for label = '" + label.name() + "' and properties = '" + propertiesString + "'");

		try (Transaction tx = database.beginTx()) {

			IndexCreator indexCreator = database.schema().indexFor(label);

			for (final String property : properties) {

				indexCreator = indexCreator.on(property);
			}

			indexDefinition = indexCreator.create();

			tx.success();
		}

		LOG.debug("created index for label = '" + label.name() + "' and property = '" + propertiesString + "'");

		LOG.debug("try to bring index online for label = '" + label.name() + "' and properties = '" + propertiesString + "'");

		try (Transaction tx = database.beginTx()) {

			database.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);

			tx.success();
		}

		LOG.debug("brought index online for label = '" + label.name() + "' and properties = '" + propertiesString + "'");

		return indexDefinition;
	}
}
