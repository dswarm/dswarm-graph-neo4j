package org.dswarm.graph.gdm.parse;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
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

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.gdm.CommonNeo4jGDMProcessor;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;

/**
 * @author tgaengler
 */
public abstract class CommonNeo4jGDMHandler implements CommonHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CommonNeo4jGDMHandler.class);

	protected int totalTriples       = 0;
	protected int addedNodes         = 0;
	protected int addedRelationships = 0;
	protected int sinceLastCommit    = 0;
	protected int i                  = 0;
	protected int literals           = 0;

	protected long tick = System.currentTimeMillis();

	protected String resourceUri;

	// TODO: init
	protected VersionHandler versionHandler = null;

	protected final CommonNeo4jGDMProcessor processor;

	public CommonNeo4jGDMHandler(final CommonNeo4jGDMProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	protected void init() {

		// TODO: init version handler
	}

	@Override
	public void setResourceUri(final String resourceUriArg) {

		resourceUri = resourceUriArg;
	}

	@Override
	public void handleStatement(final Statement st, final Resource r, final long index) throws DMPGraphException {

		// utilise r for the resource property

		i++;

		// System.out.println("handle statement " + i + ": " + st.toString());

		try {

			final org.dswarm.graph.json.Node subject = st.getSubject();

			final org.dswarm.graph.json.Predicate predicate = st.getPredicate();
			final String predicateName = predicate.getUri();

			final org.dswarm.graph.json.Node object = st.getObject();

			// Check index for subject
			// TODO: what should we do, if the subject is a resource type?
			Node subjectNode = processor.determineNode(subject, false);

			if (subjectNode == null) {

				subjectNode = processor.getDatabase().createNode();

				if (subject instanceof ResourceNode) {

					// subject is a resource node

					final String subjectURI = ((ResourceNode) subject).getUri();

					subjectNode.setProperty(GraphStatics.URI_PROPERTY, subjectURI);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

					final String dataModelURI = ((ResourceNode) subject).getDataModel();

					if (resourceUri != null && resourceUri.equals(subjectURI)) {

						versionHandler.setLatestVersion(dataModelURI);
					}

					handleSubjectDataModel(subjectNode, subjectURI, dataModelURI);

					processor.getResourcesIndex().add(subjectNode, GraphStatics.URI, subjectURI);
				} else {

					// subject is a blank node

					// note: can I expect an id here?
					processor.getBNodesIndex().put("" + subject.getId(), subjectNode);
					subjectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
				}

				addedNodes++;
			}

			if (object instanceof LiteralNode) {

				handleLiteral(r, index, st, subjectNode);
			} else { // must be Resource
				// Make sure object exists

				boolean isType = false;

				// add Label if this is a type entry
				if (predicateName.equals(RDF.type.getURI())) {

					processor.addLabel(subjectNode, ((ResourceNode) object).getUri());

					isType = true;
				}

				// Check index for object
				Node objectNode = processor.determineNode(object, isType);
				String resourceUri = null;

				if (objectNode == null) {

					objectNode = processor.getDatabase().createNode();

					if (object instanceof ResourceNode) {

						// object is a resource node

						final String objectURI = ((ResourceNode) object).getUri();
						final String dataModelURI = ((ResourceNode) object).getDataModel();

						objectNode.setProperty(GraphStatics.URI_PROPERTY, objectURI);

						if (!isType) {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());

							handleObjectDataModel(objectNode, dataModelURI);
						} else {

							objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
							processor.addLabel(objectNode, RDFS.Class.getURI());

							processor.getResourceTypesIndex().add(objectNode, GraphStatics.URI, objectURI);
						}

						processor.getResourcesIndex().add(objectNode, GraphStatics.URI, objectURI);

						addObjectToResourceWDataModelIndex(objectNode, objectURI, dataModelURI);
					} else {

						resourceUri = handleBNode(r, subject, object, subjectNode, isType, objectNode);
					}

					addedNodes++;
				}

				final String hash = processor.generateStatementHash(subjectNode, predicateName, objectNode, subject.getType(), object.getType());

				final Relationship rel = processor.getStatement(hash);
				if (rel == null) {

					addRelationship(subjectNode, objectNode, resourceUri, r, st, index, hash);
				}
			}

			totalTriples++;

			final long nodeDelta = totalTriples - sinceLastCommit;
			final long timeDelta = (System.currentTimeMillis() - tick) / 1000;

			if (nodeDelta >= 50000 || timeDelta >= 30) { // Commit every 50k operations or every 30 seconds

				processor.renewTx();

				sinceLastCommit = totalTriples;

				LOG.debug(totalTriples + " triples @ ~" + (double) nodeDelta / timeDelta + " triples/second.");

				tick = System.currentTimeMillis();
			}
		} catch (final Exception e) {

			final String message = "couldn't finish write TX successfully";

			LOG.error(message, e);

			processor.failTx();

			throw new DMPGraphException(message);
		}
	}

	protected abstract void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI);

	protected abstract void handleObjectDataModel(Node node, String dataModelURI);

	protected abstract void handleSubjectDataModel(final Node node, String URI, final String dataModelURI);

	@Override
	public void closeTransaction() {

		LOG.debug("close write TX finally");

		processor.succeedTx();
	}

	public long getCountedStatements() {

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

	protected String handleBNode(final Resource r, final org.dswarm.graph.json.Node subject, final org.dswarm.graph.json.Node object,
			final Node subjectNode, final boolean isType, final Node objectNode) {

		String resourceUri = null;

		// object is a blank node

		processor.getBNodesIndex().put("" + object.getId(), objectNode);

		if (!isType) {

			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.BNode.toString());
			resourceUri = addResourceProperty(subjectNode, subject, objectNode, r);
		} else {

			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeBNode.toString());
			processor.addLabel(objectNode, RDFS.Class.getURI());
		}
		return resourceUri;
	}

	protected void handleLiteral(final Resource r, final long index, final Statement statement, final Node subjectNode) throws DMPGraphException {

		final LiteralNode literal = (LiteralNode) statement.getObject();
		final String value = literal.getValue();

		final String hash = processor.generateStatementHash(subjectNode, statement.getPredicate().getUri(), value, statement.getSubject().getType(), statement
				.getObject().getType());

		final Relationship rel = processor.getStatement(hash);

		if (rel == null) {

			literals++;

			final Node objectNode = processor.getDatabase().createNode();
			objectNode.setProperty(GraphStatics.VALUE_PROPERTY, value);
			objectNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Literal.toString());
			processor.getValueIndex().add(objectNode, GraphStatics.VALUE, value);

			final String resourceUri = addResourceProperty(subjectNode, statement.getSubject(), objectNode, r);

			addedNodes++;

			addRelationship(subjectNode, objectNode, resourceUri, r, statement, index, hash);
		}
	}

	protected Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index) {

		final RelationshipType relType = DynamicRelationshipType.withName(statement.getPredicate().getUri());
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (statement.getOrder() != null) {

			rel.setProperty(GraphStatics.ORDER_PROPERTY, statement.getOrder());
		}

		rel.setProperty(GraphStatics.INDEX_PROPERTY, index);
		rel.setProperty(VersioningStatics.VALID_FROM_PROPERTY, versionHandler.getRange().from());
		rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getRange().to());

		if (statement.getEvidence() != null) {

			rel.setProperty(GraphStatics.EVIDENCE_PROPERTY, statement.getEvidence());
		}

		if(statement.getConfidence() != null) {

			rel.setProperty(GraphStatics.CONFIDENCE_PROPERTY, statement.getConfidence());
		}

		return rel;
	}

	protected Relationship addRelationship(final Node subjectNode, final Node objectNode, final String resourceUri, final Resource resource,
			final Statement statement, final long index, final String hash) throws DMPGraphException {

		final String finalStatementUUID;

		if (statement.getUUID() == null) {

			finalStatementUUID = UUID.randomUUID().toString();
		} else {

			finalStatementUUID = statement.getUUID();
		}

		final Relationship rel = prepareRelationship(subjectNode, objectNode, finalStatementUUID, statement, index);

		processor.getStatementIndex().add(rel, GraphStatics.HASH, hash);
		processor.addStatementToIndex(rel, finalStatementUUID);

		addedRelationships++;

		addResourceProperty(subjectNode, statement.getSubject(), rel, resourceUri, resource);

		return rel;
	}

	protected String addResourceProperty(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Node objectNode,
			final Resource resource) {

		final String resourceUri = processor.determineResourceUri(subjectNode, subject, resource);

		if (resourceUri == null) {

			return null;
		}

		objectNode.setProperty(GraphStatics.RESOURCE_PROPERTY, resourceUri);

		return resourceUri;
	}

	protected String addResourceProperty(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Relationship rel,
			final String resourceUri, final Resource resource) {

		final String finalResourceUri;

		if (resourceUri != null) {

			finalResourceUri = resourceUri;
		} else {

			finalResourceUri = processor.determineResourceUri(subjectNode, subject, resource);
		}

		rel.setProperty(GraphStatics.RESOURCE_PROPERTY, finalResourceUri);

		return finalResourceUri;
	}
}
