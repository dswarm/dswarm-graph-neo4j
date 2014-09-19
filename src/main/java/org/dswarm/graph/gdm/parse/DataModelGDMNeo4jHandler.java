package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.versioning.DataModelNeo4jVersionHandler;
import org.dswarm.graph.model.GraphStatics;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelGDMNeo4jHandler extends GDMNeo4jHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelGDMNeo4jHandler.class);

	public DataModelGDMNeo4jHandler(final GDMNeo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);
	}

	@Override protected void init() throws DMPGraphException {

		versionHandler = new DataModelNeo4jVersionHandler(processor.getProcessor());
	}

	@Override
	protected Relationship getRelationship(final String uuid) {

		final IndexHits<Relationship> hits = ((DataModelNeo4jProcessor) processor.getProcessor()).getStatementWDataModelIndex().get(
				GraphStatics.UUID_W_DATA_MODEL, ((DataModelNeo4jProcessor) processor.getProcessor()).getDataModelURI() + "." + uuid);

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
