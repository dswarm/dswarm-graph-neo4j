package org.dswarm.graph;

import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.model.GraphStatics;

import com.google.common.base.Optional;
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
public class SimpleNeo4jProcessor extends Neo4jProcessor {

	private static final Logger			LOG	= LoggerFactory.getLogger(SimpleNeo4jProcessor.class);

	protected final Index<Relationship> statementUUIDs;

	public SimpleNeo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

		super(database);

		ensureRunningTx();

		try {

			statementUUIDs = database.index().forRelationships("statement_uuids");
		} catch (final Exception e) {

			failTx();

			final String message = "couldn't load indices successfully";

			SimpleNeo4jProcessor.LOG.error(message, e);
			SimpleNeo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void addObjectToResourceWDataModelIndex(final Node node, final String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + optionalDataModelURI.get());
		}
	}

	@Override
	public void handleObjectDataModel(final Node node, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
		}
	}

	@Override
	public void handleSubjectDataModel(final Node node, String URI, final Optional<String> optionalDataModelURI) {

		if (optionalDataModelURI.isPresent()) {

			node.setProperty(GraphStatics.DATA_MODEL_PROPERTY, optionalDataModelURI.get());
			resourcesWDataModel.add(node, GraphStatics.URI_W_DATA_MODEL, URI + optionalDataModelURI.get());
		}
	}

	@Override
	public void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDs.add(rel, GraphStatics.UUID, statementUUID);
	}

	@Override
	public IndexHits<Node> getResourceNodeHits(final String resourceURI) {

		return getResourcesIndex().get(GraphStatics.URI, resourceURI);
	}
}
