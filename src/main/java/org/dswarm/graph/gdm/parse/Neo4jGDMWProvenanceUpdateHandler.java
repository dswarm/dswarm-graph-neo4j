package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Neo4jGDMWProvenanceUpdateHandler extends Neo4jBaseGDMUpdateHandler {

	private static final Logger			LOG	= LoggerFactory.getLogger(Neo4jGDMWProvenanceUpdateHandler.class);

	private final Index<Relationship>	statementUUIDsWProvenance;

	private final String				resourceGraphURI;

	public Neo4jGDMWProvenanceUpdateHandler(final GraphDatabaseService database, final String resourceGraphURIArg) throws DMPGraphException {

		super(database);

		try {

			statementUUIDsWProvenance = database.index().forRelationships("statement_uuids_w_provenance");

			resourceGraphURI = resourceGraphURIArg;

			init();
		} catch (final Exception e) {

			tx.failure();
			tx.close();

			final String message = "couldn't load indices successfully";

			Neo4jGDMWProvenanceUpdateHandler.LOG.error(message, e);

			throw new DMPGraphException(message);
		}
	}

	@Override
	protected void setLatestVersion(final String provenanceURI) throws DMPGraphException {

		final String finalProvenanceURI;

		if (provenanceURI != null) {

			finalProvenanceURI = provenanceURI;
		} else {

			finalProvenanceURI = resourceGraphURI;
		}

		super.setLatestVersion(finalProvenanceURI);
	}

	@Override
	protected void addObjectToResourceWProvenanceIndex(final Node node, final String URI, final String provenanceURI) {

		if (provenanceURI == null) {

			resourcesWProvenance.add(node, GraphStatics.URI_W_PROVENANCE, URI + resourceGraphURI);
		} else {

			resourcesWProvenance.add(node, GraphStatics.URI_W_PROVENANCE, URI + provenanceURI);
		}
	}

	@Override
	protected void handleObjectProvenance(final Node node, final String provenanceURI) {

		if (provenanceURI == null) {

			node.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
		} else {

			node.setProperty(GraphStatics.PROVENANCE_PROPERTY, provenanceURI);
		}
	}

	@Override
	protected void handleSubjectProvenance(final Node node, String URI, final String provenanceURI) {

		if (provenanceURI == null) {

			node.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);
			resourcesWProvenance.add(node, GraphStatics.URI_W_PROVENANCE, URI + resourceGraphURI);
		} else {

			node.setProperty(GraphStatics.PROVENANCE_PROPERTY, provenanceURI);
			resourcesWProvenance.add(node, GraphStatics.URI_W_PROVENANCE, URI + provenanceURI);
		}
	}

	@Override
	protected void addStatementToIndex(final Relationship rel, final String statementUUID) {

		statementUUIDsWProvenance.add(rel, GraphStatics.UUID_W_PROVENANCE, resourceGraphURI + "." + statementUUID);
	}

	@Override
	protected Relationship prepareRelationship(final Node subjectNode, final Node objectNode, final String statementUUID, final Statement statement,
			final long index) {

		final Relationship rel = super.prepareRelationship(subjectNode, objectNode, statementUUID, statement, index);

		rel.setProperty(GraphStatics.PROVENANCE_PROPERTY, resourceGraphURI);

		return rel;
	}

	@Override
	protected IndexHits<Node> getResourceNodeHits(final ResourceNode resource) {

		return resourcesWProvenance.get(GraphStatics.URI_W_PROVENANCE, resource.getUri() + resourceGraphURI);
	}

	@Override
	protected int retrieveLatestVersion() {

		int latestVersion = 1;

		final IndexHits<Node> hits = resources.get(GraphStatics.URI, resourceGraphURI);

		if (hits != null && hits.hasNext()) {

			final Node dataModelNode = hits.next();
			final Integer latestVersionFromDB = (Integer) dataModelNode.getProperty(VersioningStatics.LATEST_VERSION_PROPERTY, null);

			if (latestVersionFromDB != null) {

				latestVersion = latestVersionFromDB;
			}
		}

		if(hits != null) {

			hits.close();
		}

		return latestVersion;
	}

	@Override
	protected Relationship getRelationship(final String uuid) throws DMPGraphException {

		final IndexHits<Relationship> hits = statementUUIDsWProvenance.get(GraphStatics.UUID_W_PROVENANCE, resourceGraphURI + "." + uuid);

		if (hits != null && hits.hasNext()) {

			final Relationship rel = hits.next();

			hits.close();

			return rel;
		}

		if(hits != null) {

			hits.close();
		}

		return null;
	}

	@Override
	public void updateLatestVersion() {

		final IndexHits<Node> hits = resources.get(GraphStatics.URI, resourceGraphURI);

		if (hits != null && hits.hasNext()) {

			final Node dataModelNode = hits.next();
			dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, latestVersion);
		}

		if(hits != null) {

			hits.close();
		}
	}
}
