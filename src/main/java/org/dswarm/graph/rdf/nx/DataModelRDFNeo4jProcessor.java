package org.dswarm.graph.rdf.nx;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelRDFNeo4jProcessor extends RDFNeo4jProcessor {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelRDFNeo4jProcessor.class);

	public DataModelRDFNeo4jProcessor(final GraphDatabaseService database, final String dataModelURIArg) throws DMPGraphException {

		super(new DataModelNeo4jProcessor(database, dataModelURIArg));
	}
}
