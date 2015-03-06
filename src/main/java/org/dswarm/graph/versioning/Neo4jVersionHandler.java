/**
 * This file is part of d:swarm graph extension.
 *
 * d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * d:swarm graph extension is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with d:swarm graph extension.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.versioning;

import java.util.UUID;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * @author tgaengler
 */
public abstract class Neo4jVersionHandler implements VersionHandler {

	private static final Logger LOG = LoggerFactory.getLogger(Neo4jVersionHandler.class);

	protected boolean latestVersionInitialized = false;

	protected int latestVersion;

	private Range range;

	protected final Neo4jProcessor processor;

	public Neo4jVersionHandler(final Neo4jProcessor processorArg) throws DMPGraphException {

		processor = processorArg;
	}

	@Override
	public int getLatestVersion() {

		return latestVersion;
	}

	@Override public Range getRange() {

		return range;
	}

	protected void init() {

		latestVersion = retrieveLatestVersion() + 1;
		range = Range.range(latestVersion);
	}

	protected abstract int retrieveLatestVersion();

	public void setLatestVersion(final Optional<String> optionalDataModelURI) throws DMPGraphException {

		if (!latestVersionInitialized) {

			if (!optionalDataModelURI.isPresent()) {

				return;
			}

			Optional<Node> optionalDataModelNode = processor.determineNode(Optional.of(NodeType.Resource), Optional.<String>absent(),
					optionalDataModelURI, Optional.of(VersioningStatics.VERSIONING_DATA_MODEL_URI));

			if (optionalDataModelNode.isPresent()) {

				latestVersionInitialized = true;

				return;
			}

			final Node dataModelNode = processor.getDatabase().createNode();
			processor.addLabel(dataModelNode, VersioningStatics.DATA_MODEL_TYPE);
			dataModelNode.setProperty(GraphStatics.URI_PROPERTY, optionalDataModelURI.get());
			dataModelNode.setProperty(GraphStatics.DATA_MODEL_PROPERTY, VersioningStatics.VERSIONING_DATA_MODEL_URI);
			dataModelNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.Resource.toString());
			dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, range.from());

			processor.addNodeToResourcesWDataModelIndex(optionalDataModelURI.get(), VersioningStatics.VERSIONING_DATA_MODEL_URI, dataModelNode);

			Optional<Node> optionaDataModelTypeNode = processor.determineNode(Optional.of(NodeType.TypeResource), Optional.<String>absent(), Optional.of(
					VersioningStatics.DATA_MODEL_TYPE), Optional.<String>absent());

			final Node dataModelTypeNode;

			if (optionaDataModelTypeNode.isPresent()) {

				dataModelTypeNode = optionaDataModelTypeNode.get();
			} else {

				dataModelTypeNode = processor.getDatabase().createNode();
				processor.addLabel(dataModelTypeNode, RDFS.Class.getURI());
				dataModelTypeNode.setProperty(GraphStatics.URI_PROPERTY, VersioningStatics.DATA_MODEL_TYPE);
				dataModelTypeNode.setProperty(GraphStatics.NODETYPE_PROPERTY, NodeType.TypeResource.toString());

				processor.addNodeToResourceTypesIndex(VersioningStatics.DATA_MODEL_TYPE, dataModelTypeNode);
			}

			final long hash = processor.generateStatementHash(dataModelNode, RDF.type.getURI(), dataModelTypeNode, NodeType.Resource, NodeType.Resource);

			Relationship rel = processor.getStatement(hash);

			if (rel == null) {

				final RelationshipType relType = DynamicRelationshipType.withName(RDF.type.getURI());
				rel = dataModelNode.createRelationshipTo(dataModelTypeNode, relType);
				rel.setProperty(GraphStatics.INDEX_PROPERTY, 0);
				rel.setProperty(GraphStatics.DATA_MODEL_PROPERTY, VersioningStatics.VERSIONING_DATA_MODEL_URI);

				final String uuid = UUID.randomUUID().toString();

				rel.setProperty(GraphStatics.UUID_PROPERTY, uuid);

				processor.getStatementHashesIndex().acquireUsing(hash, rel.getId());
				processor.addStatementToIndex(rel, uuid);
			}

			latestVersionInitialized = true;
		}
	}

}
