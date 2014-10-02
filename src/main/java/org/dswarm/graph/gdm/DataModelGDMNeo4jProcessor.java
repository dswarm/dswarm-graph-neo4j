package org.dswarm.graph.gdm;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelGDMNeo4jProcessor extends GDMNeo4jProcessor {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelGDMNeo4jProcessor.class);

	public DataModelGDMNeo4jProcessor(final GraphDatabaseService database, final String dataModelURIArg) throws DMPGraphException {

		super(new DataModelNeo4jProcessor(database, dataModelURIArg));
	}
}
