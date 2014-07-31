package org.dswarm.graph.gdm.read;

import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

/**
 * Created by tgaengler on 31/07/14.
 */
public class PropertyGraphGDMResourceByURIReader extends PropertyGraphGDMResourceReader {

	private final String	recordUri;

	public PropertyGraphGDMResourceByURIReader(final String recordUriArg, final String resourceGraphUri, final GraphDatabaseService database) {

		super(resourceGraphUri, database);

		recordUri = recordUriArg;
	}

	@Override
	protected Node getResourceNode() {

		final Index<Node> resourcesWProvenance = database.index().forNodes("resources_w_provenance");
		final IndexHits<Node> hits = resourcesWProvenance.get(GraphStatics.URI_W_PROVENANCE, recordUri + resourceGraphUri);

		if (hits == null) {

			return null;
		}
		if (!hits.hasNext()) {

			return null;
		}

		return hits.next();
	}
}
