package org.dswarm.graph.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.DataModelNeo4jVersionHandler;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelNeo4jHandler extends BaseNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelNeo4jHandler.class);

	public DataModelNeo4jHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override
	protected void init() throws DMPGraphException {

		versionHandler = new DataModelNeo4jVersionHandler(processor);
	}

	@Override public Relationship getRelationship(final String uuid) {

		final IndexHits<Relationship> hits = ((DataModelNeo4jProcessor) processor).getStatementWDataModelIndex().get(GraphStatics.UUID_W_DATA_MODEL,
				((DataModelNeo4jProcessor) processor).getDataModelURI() + "." + uuid);

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
