package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.gdm.versioning.SimpleGDMNeo4jVersionHandler;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public class SimpleGDMNeo4jHandler extends GDMNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(SimpleGDMNeo4jHandler.class);

	public SimpleGDMNeo4jHandler(final GDMNeo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new SimpleGDMNeo4jVersionHandler(processor);
	}

	@Override
	protected Relationship getRelationship(final String uuid) {

		final IndexHits<Relationship> hits = processor.getProcessor().getStatementIndex().get(GraphStatics.UUID, uuid);

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
