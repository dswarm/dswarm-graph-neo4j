package org.dswarm.graph.gdm.read;

import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.utils.GraphUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMReader {

	private static final Logger					LOG				= LoggerFactory.getLogger(PropertyGraphGDMReader.class);

	final Map<Long, org.dswarm.graph.json.Node>	bnodes			= new HashMap<>();
	final Map<String, ResourceNode>                resourceNodes     = new HashMap<>();
	final Map<String, Predicate> predicates = new HashMap<>();

	public org.dswarm.graph.json.Node readSubject(final Node subjectNode) throws DMPGraphException {

		final long subjectId = subjectNode.getId();
		final NodeType subjectNodeType = GraphUtils.determineNodeType(subjectNode);

		final org.dswarm.graph.json.Node subjectGDMNode;

		switch (subjectNodeType) {

			case Resource:
			case TypeResource:

				subjectGDMNode = readResource(subjectNode);

				break;
			case BNode:
			case TypeBNode:

				subjectGDMNode = createResourceFromBNode(subjectId);

				break;
			default:

				final String message = "subject node type can only be a resource (or type resource) or bnode (or type bnode)";

				PropertyGraphGDMReader.LOG.error(message);

				throw new DMPGraphException(message);
		}

		return subjectGDMNode;
	}

	public org.dswarm.graph.json.Node readObject(final Node objectNode) throws DMPGraphException {

		final long objectId = objectNode.getId();
		final NodeType objectNodeType = GraphUtils.determineNodeType(objectNode);

		final org.dswarm.graph.json.Node objectGDMNode;

		switch (objectNodeType) {

			case Resource:
			case TypeResource:

				objectGDMNode = readResource(objectNode);

				break;
			case BNode:
			case TypeBNode:

				objectGDMNode = createResourceFromBNode(objectId);

				break;
			case Literal:

				final String object = (String) objectNode.getProperty(GraphStatics.VALUE_PROPERTY, null);

				if (object == null) {

					final String message = "object value can't be null";

					PropertyGraphGDMReader.LOG.error(message);

					throw new DMPGraphException(message);
				}

				objectGDMNode = new LiteralNode(objectId, object);

				break;
			default:

				final String message = "unknown node type " + objectNodeType.getName() + " for object node";

				PropertyGraphGDMReader.LOG.error(message);

				throw new DMPGraphException(message);
		}

		return objectGDMNode;
	}

	public Statement readStatement(final Relationship rel) throws DMPGraphException {

		final org.dswarm.graph.json.Node subject = readObject(rel.getStartNode());
		final Predicate predicate = getPredicate(rel.getType().name());
		final org.dswarm.graph.json.Node object = readObject(rel.getEndNode());
		final String uuid = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);
		final Long order = (Long) rel.getProperty(GraphStatics.ORDER_PROPERTY, null);
		final String confidence = (String) rel.getProperty(GraphStatics.CONFIDENCE_PROPERTY, null);
		final String evidence = (String) rel.getProperty(GraphStatics.EVIDENCE_PROPERTY, null);

		final Statement statement = new Statement(subject, predicate, object);
		statement.setOrder(order);

		if(uuid != null) {

			statement.setUUID(uuid);
		}

		if(confidence != null) {

			statement.setConfidence(confidence);
		}

		if(evidence != null) {

			statement.setEvidence(evidence);
		}

		return statement;
	}

	private Predicate getPredicate(final String predicateName) {

		if(!predicates.containsKey(predicateName)) {

			predicates.put(predicateName, new Predicate(predicateName));
		}

		return predicates.get(predicateName);
	}


	private ResourceNode readResource(final Node node) throws DMPGraphException {

		final String resourceURI = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);

		if (resourceURI == null) {

			final String message = "resource URI can't be null";

			PropertyGraphGDMReader.LOG.error(message);

			throw new DMPGraphException(message);
		}

		final String dataModelURI = (String) node.getProperty(GraphStatics.DATA_MODEL_PROPERTY, null);

		final ResourceNode resourceNode;

		if (dataModelURI == null) {

			resourceNode = createResourceFromURI(node.getId(), resourceURI);
		} else {

			resourceNode = createResourceFromURIAndDataModel(node.getId(), resourceURI, dataModelURI);
		}

		return resourceNode;
	}

	private org.dswarm.graph.json.Node createResourceFromBNode(final long bnodeId) {

		if (!bnodes.containsKey(bnodeId)) {

			bnodes.put(bnodeId, new org.dswarm.graph.json.Node(bnodeId));
		}

		return bnodes.get(bnodeId);
	}

	private ResourceNode createResourceFromURI(final long id, final String uri) {

		if (!resourceNodes.containsKey(uri)) {

			resourceNodes.put(uri, new ResourceNode(id, uri));
		}

		return resourceNodes.get(uri);
	}

	private ResourceNode createResourceFromURIAndDataModel(final long id, final String uri, final String dataModel) {

		if (!resourceNodes.containsKey(uri + dataModel)) {

			resourceNodes.put(uri + dataModel, new ResourceNode(id, uri, dataModel));
		}

		return resourceNodes.get(uri + dataModel);
	}



}
