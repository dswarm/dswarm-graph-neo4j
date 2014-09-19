package org.dswarm.graph.gdm;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public abstract class GDMNeo4jProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(GDMNeo4jProcessor.class);

	protected final Neo4jProcessor processor;

	public GDMNeo4jProcessor(final Neo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	public Neo4jProcessor getProcessor() {

		return processor;
	}
	
	public Node determineNode(final org.dswarm.graph.json.Node resource, final boolean isType) {

		final Node node;

		if (resource instanceof ResourceNode) {

			// resource node

			final IndexHits<Node> hits;

			if (!isType) {

				if (((ResourceNode) resource).getDataModel() == null) {

					hits = getResourceNodeHits((ResourceNode) resource);
				} else {

					hits = processor.getResourcesWDataModelIndex().get(GraphStatics.URI_W_DATA_MODEL,
							((ResourceNode) resource).getUri() + ((ResourceNode) resource).getDataModel());
				}
			} else {

				hits = processor.getResourceTypesIndex().get(GraphStatics.URI, ((ResourceNode) resource).getUri());
			}

			if (hits != null && hits.hasNext()) {

				// node exists

				node = hits.next();

				hits.close();

				return node;
			}

			if(hits != null) {

				hits.close();
			}

			return null;
		}

		if (resource instanceof LiteralNode) {

			// literal node - should never be the case

			return null;
		}

		// resource must be a blank node

		node = processor.getBNodesIndex().get("" + resource.getId());

		return node;
	}

	public String determineResourceUri(final Node subjectNode, final org.dswarm.graph.json.Node subject, final Resource resource) {

		final Long nodeId = subjectNode.getId();

		final String resourceUri;

		if (processor.getNodeResourceMap().containsKey(nodeId)) {

			resourceUri = processor.getNodeResourceMap().get(nodeId);
		} else {

			if (subject instanceof ResourceNode) {

				resourceUri = ((ResourceNode) subject).getUri();
			} else {

				resourceUri = resource.getUri();
			}

			processor.getNodeResourceMap().put(nodeId, resourceUri);
		}

		return resourceUri;
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final Node objectNode,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);
		final String objectIdentifier = getIdentifier(objectNode, objectNodeType);

		return generateStatementHash(predicateName, subjectNodeType, objectNodeType, subjectIdentifier, objectIdentifier);
	}

	public String generateStatementHash(final Node subjectNode, final String predicateName, final String objectValue,
			final org.dswarm.graph.json.NodeType subjectNodeType, final org.dswarm.graph.json.NodeType objectNodeType) throws DMPGraphException {

		final String subjectIdentifier = getIdentifier(subjectNode, subjectNodeType);

		return generateStatementHash(predicateName, subjectNodeType, objectNodeType, subjectIdentifier, objectValue);
	}

	public Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index, final VersionHandler versionHandler) {

		final RelationshipType relType = DynamicRelationshipType.withName(statement.getPredicate().getUri());
		final Relationship rel = subjectNode.createRelationshipTo(objectNode, relType);

		rel.setProperty(GraphStatics.UUID_PROPERTY, statementUUID);

		if (statement.getOrder() != null) {

			rel.setProperty(GraphStatics.ORDER_PROPERTY, statement.getOrder());
		}

		rel.setProperty(GraphStatics.INDEX_PROPERTY, index);

		// TODO: versioning handling only implemented for data models right now

		if (statement.getEvidence() != null) {

			rel.setProperty(GraphStatics.EVIDENCE_PROPERTY, statement.getEvidence());
		}

		if(statement.getConfidence() != null) {

			rel.setProperty(GraphStatics.CONFIDENCE_PROPERTY, statement.getConfidence());
		}

		return rel;
	}

	protected abstract IndexHits<Node> getResourceNodeHits(final ResourceNode resource);

	private String generateStatementHash(final String predicateName, final org.dswarm.graph.json.NodeType subjectNodeType,
			final org.dswarm.graph.json.NodeType objectNodeType, final String subjectIdentifier, final String objectIdentifier)
			throws DMPGraphException {

		final StringBuffer sb = new StringBuffer();

		sb.append(subjectNodeType.toString()).append(":").append(subjectIdentifier).append(" ").append(predicateName).append(" ")
				.append(objectNodeType.toString()).append(":").append(objectIdentifier).append(" ");

		MessageDigest messageDigest = null;

		try {

			messageDigest = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e) {

			throw new DMPGraphException("couldn't instantiate hash algo");
		}
		messageDigest.update(sb.toString().getBytes());

		return new String(messageDigest.digest());
	}

	private String getIdentifier(final Node node, final org.dswarm.graph.json.NodeType nodeType) {

		final String identifier;

		switch (nodeType) {

			case Resource:

				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				final String dataModel = (String) node.getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

				if (dataModel == null) {

					identifier = uri;
				} else {

					identifier = uri + dataModel;
				}

				break;
			case BNode:

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
}
