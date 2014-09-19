package org.dswarm.graph;

import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public abstract class Neo4jProcessor {

	private static final Logger				LOG			= LoggerFactory.getLogger(Neo4jProcessor.class);

	protected int addedLabels = 0;

	protected final GraphDatabaseService database;
	protected final Index<Node>          resources;
	protected final Index<Node>          resourcesWDataModel;
	protected final Index<Node>          resourceTypes;
	protected final Index<Node>          values;
	protected final Map<String, Node>    bnodes;
	protected final Index<Relationship>  statementHashes;
	protected final Map<Long, String>    nodeResourceMap;

	protected Transaction tx;

	boolean txIsClosed = false;

	public Neo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

		this.database = database;
		beginTx();

		try {

			LOG.debug("start write TX");

			resources = database.index().forNodes("resources");
			resourcesWDataModel = database.index().forNodes("resources_w_data_model");
			resourceTypes = database.index().forNodes("resource_types");
			values = database.index().forNodes("values");
			bnodes = new HashMap<>();
			statementHashes = database.index().forRelationships("statement_hashes");
			nodeResourceMap = new HashMap<>();

		} catch (final Exception e) {

			failTx();

			final String message = "couldn't load indices successfully";

			Neo4jProcessor.LOG.error(message, e);
			Neo4jProcessor.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	public GraphDatabaseService getDatabase() {

		return database;
	}

	public Index<Node> getResourcesIndex() {

		return resources;
	}

	public Index<Node> getResourcesWDataModelIndex() {

		return resourcesWDataModel;
	}

	public Map<String, Node> getBNodesIndex() {

		return bnodes;
	}

	public Index<Node> getResourceTypesIndex() {

		return resourceTypes;
	}

	public Index<Node> getValueIndex() {

		return values;
	}

	public Index<Relationship> getStatementIndex() {

		return statementHashes;
	}

	public Map<Long, String> getNodeResourceMap() {

		return nodeResourceMap;
	}

	public void beginTx() {

		tx = database.beginTx();
		txIsClosed = false;

		Neo4jProcessor.LOG.debug("begin new tx");
	}

	public void renewTx() {

		succeedTx();
		beginTx();
	}

	public void failTx() {

		Neo4jProcessor.LOG.error("tx failed; close tx");

		tx.failure();
		tx.close();
		txIsClosed = true;
	}

	public void succeedTx() {

		Neo4jProcessor.LOG.debug("tx succeeded; close tx");

		tx.success();
		tx.close();
		txIsClosed = true;
	}

	public void ensureRunningTx() {

		if (txIsClosed()) {

			beginTx();
		}
	}

	public boolean txIsClosed() {

		return txIsClosed;
	}

	public void addLabel(final Node node, final String labelString) {

		final Label label = DynamicLabel.label(labelString);
		boolean hit = false;
		final Iterable<Label> labels = node.getLabels();

		for (final Label lbl : labels) {

			if (label.equals(lbl)) {

				hit = true;
				break;
			}
		}

		if (!hit) {

			node.addLabel(label);
			addedLabels++;
		}
	}

	public Relationship getStatement(final String hash) throws DMPGraphException {

		IndexHits<Relationship> hits = statementHashes.get(GraphStatics.HASH, hash);

		if (hits != null && hits.hasNext()) {

			final Relationship rel = hits.next();

			hits.close();

			return rel;
		}

		if (hits != null) {

			hits.close();
		}

		return null;
	}

	public abstract void addObjectToResourceWDataModelIndex(final Node node, final String URI, final String dataModelURI);

	public abstract void handleObjectDataModel(Node node, String dataModelURI);

	public abstract void handleSubjectDataModel(final Node node, String URI, final String dataModelURI);

	public abstract void addStatementToIndex(final Relationship rel, final String statementUUID);
}
