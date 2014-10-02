package org.dswarm.graph.batch.rdf.pnx;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.batch.DataModelNeo4jProcessor;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelRDFNeo4jProcessor extends RDFNeo4jProcessor {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelRDFNeo4jProcessor.class);

	public DataModelRDFNeo4jProcessor(final BatchInserter inserter, final String dataModelURIArg) throws DMPGraphException {

		super(new DataModelNeo4jProcessor(inserter, dataModelURIArg));
	}
}
