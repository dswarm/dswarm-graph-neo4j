package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.BaseNeo4jGDMProcessor;
import org.dswarm.graph.gdm.Neo4jGDMProcessor;
import org.dswarm.graph.gdm.Neo4jGDMWDataModelProcessor;
import org.dswarm.graph.gdm.versioning.Neo4jGDMVersionHandler;
import org.dswarm.graph.model.GraphStatics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public class Neo4jGDMHandler extends BaseNeo4jGDMHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(Neo4jGDMHandler.class);

	public Neo4jGDMHandler(final BaseNeo4jGDMProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new Neo4jGDMVersionHandler(processor);
	}

	@Override
	protected Relationship getRelationship(final String uuid) {

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
