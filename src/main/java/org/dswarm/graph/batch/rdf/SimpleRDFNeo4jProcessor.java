package org.dswarm.graph.batch.rdf;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.batch.SimpleNeo4jProcessor;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class SimpleRDFNeo4jProcessor extends RDFNeo4jProcessor {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleRDFNeo4jProcessor.class);

	public SimpleRDFNeo4jProcessor(final BatchInserter inserter) throws DMPGraphException {

		super(new SimpleNeo4jProcessor(inserter));
	}
}
