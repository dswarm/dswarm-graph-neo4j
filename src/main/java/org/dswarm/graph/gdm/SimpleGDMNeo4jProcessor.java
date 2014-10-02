package org.dswarm.graph.gdm;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.SimpleNeo4jProcessor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class SimpleGDMNeo4jProcessor extends GDMNeo4jProcessor {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleGDMNeo4jProcessor.class);

	public SimpleGDMNeo4jProcessor(final GraphDatabaseService database) throws DMPGraphException {

		super(new SimpleNeo4jProcessor(database));
	}
}
