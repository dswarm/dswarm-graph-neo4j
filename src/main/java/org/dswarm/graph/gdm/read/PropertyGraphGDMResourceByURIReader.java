package org.dswarm.graph.gdm.read;

import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMResourceByURIReader extends PropertyGraphGDMResourceReader {

	private final String	recordUri;

	public PropertyGraphGDMResourceByURIReader(final String recordUriArg, final String dataModelUri, final GraphDatabaseService database) {

		super(dataModelUri, database);

		recordUri = recordUriArg;
	}

	@Override
	protected Node getResourceNode() {

		final Index<Node> resourcesWDataModel = database.index().forNodes("resources_w_data_model");
		final IndexHits<Node> hits = resourcesWDataModel.get(GraphStatics.URI_W_DATA_MODEL, recordUri + dataModelUri);

		if (hits == null) {

			return null;
		}
		if (!hits.hasNext()) {

			hits.close();

			return null;
		}

		final Node node = hits.next();

		hits.close();

		return node;
	}
}
