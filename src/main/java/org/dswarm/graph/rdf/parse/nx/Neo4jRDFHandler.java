package org.dswarm.graph.rdf.parse.nx;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.NodeType;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public class Neo4jRDFHandler implements RDFHandler {

	private static final Logger			LOG						= LoggerFactory.getLogger(Neo4jRDFHandler.class);

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
	// private final Index<Relationship>	statements;
	// private final Map<Long, String> nodeResourceMap;

	private Transaction					tx;

	private Label						resourceNodeLabel		= DynamicLabel.label(NodeType.Resource.toString());
	private Label						typeResourceNodeLabel	= DynamicLabel.label(NodeType.TypeResource.toString());
	private Label						literalNodeLabel		= DynamicLabel.label(NodeType.Literal.toString());

	public Neo4jRDFHandler(final GraphDatabaseService database) throws DMPGraphException {

		this.database = database;

		getOrCreateIndex(DynamicLabel.label(NodeType.Resource.toString()), GraphStatics.URI_PROPERTY);
		getOrCreateIndex(DynamicLabel.label(NodeType.TypeResource.toString()), GraphStatics.URI_PROPERTY);
		getOrCreateIndex(DynamicLabel.label(NodeType.Literal.toString()), GraphStatics.VALUE_PROPERTY);

		// resources = database.index().forNodes("resources");
		// resourcesWProvenance = database.index().forNodes("resources_w_provenance");
		// resourceTypes = database.index().forNodes("resource_types");
		// values = database.index().forNodes("values");
		bnodes = new HashMap<>();

		// TODO: switch to auto-index for nodes and relationships, i.e., remove separate statements index + enable auto-indexing => via uuid property ;)

//		try (Transaction tx = database.beginTx()) {
//
//			statements = database.index().forRelationships("statements");
//			tx.success();
//		}

		// nodeResourceMap = new HashMap<Long, String>();

		tx = database.beginTx();

		LOG.debug("start write TX");
	}

	@Override
	public void handleStatement(final org.semanticweb.yars.nx.Node[] st) throws DMPGraphException {

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
					// subjectNode.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
					// resources.add(subjectNode, GraphStatics.URI, subject.toString());
					// resourcesWProvenance.add(subjectNode, GraphStatics.URI_W_PROVENANCE, subject.toString() +
					// resourceGraphURI);
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
					} else {
						if (!isType) {

							objectNode = database.createNode(resourceNodeLabel);

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
						} else {

							objectNode = database.createNode(resourceNodeLabel, typeResourceNodeLabel);

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
							addLabel(objectNode, RDFS.CLASS.toString());
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

			final String message = "couldn't finish write TX successfully";

			LOG.error(message, e);

			tx.failure();
			tx.close();

			throw new DMPGraphException(message);
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
		// rel.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
		// statements.add(rel, GraphStatics.ID, Long.valueOf(rel.getId()));

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

		ResourceIterable<Node> hits = database.findNodesByLabelAndProperty(resourceNodeLabel, GraphStatics.URI_PROPERTY, resource.toString());

		if(hits == null) {

			return null;
		}

		final ResourceIterator<Node> iterator = hits.iterator();

		if (iterator != null && iterator.hasNext()) {

			// node exists

			node = iterator.next();

			iterator.close();

			return node;
		}

		if(iterator != null) {

			iterator.close();
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

	private IndexDefinition getOrCreateIndex(final Label label, final String property) throws DMPGraphException {

		IndexDefinition indexDefinition = null;

		LOG.debug("try to find index for label = '" + label.name() + "' and property = '" + property + "'");

		try (final Transaction tx = database.beginTx()) {

			Iterable<IndexDefinition> indices = database.schema().getIndexes(label);

			if (indices != null && indices.iterator().hasNext()) {

				indexDefinition = indices.iterator().next();
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't find schema index successfully";

			Neo4jRDFHandler.LOG.error(message, e);
			Neo4jRDFHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}

		if (indexDefinition != null) {

			LOG.debug("found existing index for label = '" + label.name() + "' and property = '" + property + "'");

			return indexDefinition;
		}

		return createIndex(label, property);
	}

	private IndexDefinition createIndex(final Label label, final String property) throws DMPGraphException {

		IndexDefinition indexDefinition = null;

		LOG.debug("try to create index for label = '" + label.name() + "' and property = '" + property + "'");

		try (Transaction tx = database.beginTx()) {

			final IndexCreator indexCreator = database.schema().indexFor(label).on(property);
			indexDefinition = indexCreator.create();
			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't create schema index successfully";

			Neo4jRDFHandler.LOG.error(message, e);
			Neo4jRDFHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}

		LOG.debug("created index for label = '" + label.name() + "' and property = '" + property + "'");

		LOG.debug("try to bring index online for label = '" + label.name() + "' and property = '" + property + "'");

		try (Transaction tx = database.beginTx()) {

			database.schema().awaitIndexOnline(indexDefinition, 5, TimeUnit.SECONDS);
			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't bring schema index successfully online";

			Neo4jRDFHandler.LOG.error(message, e);
			Neo4jRDFHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}

		LOG.debug("brought index online for label = '" + label.name() + "' and property = '" + property + "'");

		return indexDefinition;
	}
}
