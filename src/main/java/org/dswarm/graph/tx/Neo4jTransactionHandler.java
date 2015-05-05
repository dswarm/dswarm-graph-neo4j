package org.dswarm.graph.tx;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Neo4jTransactionHandler implements TransactionHandler {

	private static final Logger LOG = LoggerFactory.getLogger(Neo4jTransactionHandler.class);

	private final GraphDatabaseService database;

	private Transaction tx;

	boolean txIsClosed = true;

	public Neo4jTransactionHandler(final GraphDatabaseService databaseArg) {

		database = databaseArg;
	}

	@Override public void beginTx() {

		LOG.debug("beginning new tx");

		tx = database.beginTx();
		txIsClosed = false;

		LOG.debug("new tx is ready");
	}

	@Override public void renewTx() {

		succeedTx();
		beginTx();
	}

	@Override public void failTx() {

		if (tx != null) {

			LOG.error("tx failed; closing tx");

			tx.failure();
			tx.close();
			tx = null;
			txIsClosed = true;

			LOG.error("tx failed; closed tx");
		} else {

			LOG.debug("failed tx is already closed");
		}
	}

	@Override public void succeedTx() {

		if (tx != null) {

			LOG.debug("tx succeeded; closing tx");

			tx.success();
			tx.close();
			tx = null;
			txIsClosed = true;

			LOG.debug("tx succeeded; closed tx");
		} else {

			LOG.debug("succeeded tx is already closed");
		}
	}

	@Override public void ensureRunningTx() {

		if (txIsClosed()) {

			beginTx();
		}
	}

	@Override public boolean txIsClosed() {

		return txIsClosed;
	}
}
