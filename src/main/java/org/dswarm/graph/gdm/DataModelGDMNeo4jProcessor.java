package org.dswarm.graph.gdm;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersionHandler;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
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

	@Override
	public Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index, final VersionHandler versionHandler) {

		final Relationship rel = super.prepareRelationship(subjectNode, objectNode, statementUUID, statement, index, versionHandler);

		rel.setProperty(GraphStatics.DATA_MODEL_PROPERTY, ((DataModelNeo4jProcessor) processor).getDataModelURI());

		rel.setProperty(VersioningStatics.VALID_FROM_PROPERTY, versionHandler.getRange().from());
		rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getRange().to());

		return rel;
	}

	@Override
	protected IndexHits<Node> getResourceNodeHits(final ResourceNode resource) {

		return processor.getResourcesWDataModelIndex().get(GraphStatics.URI_W_DATA_MODEL,
				resource.getUri() + ((DataModelNeo4jProcessor) processor).getDataModelURI());
	}
}
