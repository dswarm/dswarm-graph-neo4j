package org.dswarm.graph.tx;

/**
 * @author tgaengler
 */
public interface TransactionHandler {

	void beginTx();

	void renewTx();

	void failTx();

	void succeedTx();

	void ensureRunningTx();

	boolean txIsClosed();
}
