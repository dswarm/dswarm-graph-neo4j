package org.dswarm.graph.gdm;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Neo4jGDMProcessor extends BaseNeo4jGDMProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(Neo4jGDMProcessor.class);

	protected final Index<Relationship>	statementUUIDs;

	public Neo4jGDMProcessor(final GraphDatabaseService database) throws DMPGraphException {

		super(database);

		ensureRunningTx();

		try {

			statementUUIDs = database.index().forRelationships("statement_uuids");
		} catch (final Exception e) {

			tx.failure();
			tx.close();

			final String message = "couldn't load indices successfully";

			Neo4jGDMProcessor.LOG.error(message, e);
			Neo4jGDMProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI) {

		if (dataModelURI != null) {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + dataModelURI);
		}
	}

	@Override
	public void handleObjectDataModel(final Node node, final String dataModelURI) {

		if (dataModelURI != null) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
		}
	}

	@Override
	public void handleSubjectDataModel(final Node node, String URI, final String dataModelURI) {

		if (dataModelURI != null) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, dataModelURI);
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + dataModelURI);
		}
	}

	@Override
	public void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDs.add(rel, GraphStatics.UUID, statementUUID);
	}

	@Override
	protected IndexHits<Node> getResourceNodeHits(final ResourceNode resource) {

		return resources.get(GraphStatics.URI, resource.getUri());
	}
}
