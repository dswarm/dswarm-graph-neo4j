package org.dswarm.graph.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.SimpleNeo4jVersionHandler;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public class SimpleNeo4jHandler extends BaseNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleNeo4jHandler.class);

	public SimpleNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new SimpleNeo4jVersionHandler(processor);
	}

	@Override public Relationship getRelationship(final String uuid) {

		final IndexHits<Relationship> hits = processor.getStatementIndex().get(GraphStatics.UUID, uuid);

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
}
