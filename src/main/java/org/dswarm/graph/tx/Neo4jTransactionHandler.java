/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
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
