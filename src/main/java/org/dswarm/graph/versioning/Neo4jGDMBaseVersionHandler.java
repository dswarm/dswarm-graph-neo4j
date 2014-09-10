package org.dswarm.graph.versioning;

import java.util.UUID;

import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.gdm.CommonNeo4jGDMProcessor;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public abstract class Neo4jGDMBaseVersionHandler implements VersionHandler {

	private static final Logger LOG = LoggerFactory.getLogger(Neo4jGDMBaseVersionHandler.class);

	protected boolean latestVersionInitialized = false;

	protected int latestVersion;

	private Range range;

	protected final CommonNeo4jGDMProcessor processor;

	public Neo4jGDMBaseVersionHandler(final CommonNeo4jGDMProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	@Override
	public int getLatestVersion() {

		return latestVersion;
	}

	protected void init() {

		latestVersion = retrieveLatestVersion() + 1;
		range = Range.range(latestVersion);
	}

	protected abstract int retrieveLatestVersion();

	public void setLatestVersion(final String dataModelURI) throws DMPGraphException {

		if (!latestVersionInitialized) {

			if (dataModelURI == null) {

				return;
			}

			Node dataModelNode = processor.determineNode(new ResourceNode(dataModelURI), false);

			if (dataModelNode != null) {

				latestVersionInitialized = true;

				return;
			}

			dataModelNode = processor.getDatabase().createNode();
			processor.addLabel(dataModelNode, VersioningStatics.DATA_MODEL_TYPE);
			dataModelNode.setProperty(GraphStatics.URI_PROPERTY, dataModelURI);
			dataModelNode.setProperty(GraphStatics.DATA_MODEL_PROPERTY, VersioningStatics.VERSIONING_DATA_MODEL_URI);
			dataModelNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
			dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, range.from());

			processor.getResourcesIndex().add(dataModelNode, GraphStatics.URI, dataModelURI);
			processor.getResourcesWDataModelIndex().add(dataModelNode, GraphStatics.URI_W_DATA_MODEL, dataModelURI + VersioningStatics.VERSIONING_DATA_MODEL_URI);

			Node dataModelTypeNode = processor.determineNode(new ResourceNode(VersioningStatics.DATA_MODEL_TYPE), true);

			if (dataModelTypeNode == null) {

				dataModelTypeNode = processor.getDatabase().createNode();
				processor.addLabel(dataModelTypeNode, RDFS.Class.getURI());
				dataModelTypeNode.setProperty(GraphStatics.URI_PROPERTY, VersioningStatics.DATA_MODEL_TYPE);
				dataModelTypeNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());
			}

			final String hash = processor.generateStatementHash(dataModelNode, RDF.type.getURI(), dataModelTypeNode, org.dswarm.graph.json.NodeType.Resource,
					org.dswarm.graph.json.NodeType.Resource);

			Relationship rel = processor.getStatement(hash);

			if (rel == null) {

				final RelationshipType relType = DynamicRelationshipType.withName(RDF.type.getURI());
				rel = dataModelNode.createRelationshipTo(dataModelTypeNode, relType);
				rel.setProperty(GraphStatics.INDEX_PROPERTY, 0);
				rel.setProperty(GraphStatics.DATA_MODEL_PROPERTY, VersioningStatics.VERSIONING_DATA_MODEL_URI);

				final String uuid = UUID.randomUUID().toString();

				rel.setProperty(GraphStatics.UUID_PROPERTY, uuid);

				processor.getStatementIndex().add(rel, GraphStatics.HASH, hash);
				processor.addStatementToIndex(rel, uuid);
			}

			latestVersionInitialized = true;
		}
	}

}
