package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.gdm.BaseNeo4jGDMProcessor;
import org.dswarm.graph.gdm.Neo4jGDMWDataModelProcessor;
import org.dswarm.graph.gdm.versioning.Neo4jGDMWDataModelVersionHandler;
import org.dswarm.graph.model.GraphStatics;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Neo4jGDMWDataModelHandler extends BaseNeo4jGDMHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(Neo4jGDMWDataModelHandler.class);

	public Neo4jGDMWDataModelHandler(final BaseNeo4jGDMProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override protected void init() throws DMPGraphException {

		versionHandler = new Neo4jGDMWDataModelVersionHandler(processor);
	}

	@Override
	protected Relationship getRelationship(final String uuid) {

		final IndexHits<Relationship> hits = ((Neo4jGDMWDataModelProcessor) processor).getStatementWDataModelIndex().get(
				GraphStatics.UUID_W_DATA_MODEL, ((Neo4jGDMWDataModelProcessor) processor).getDataModelURI() + "." + uuid);

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
