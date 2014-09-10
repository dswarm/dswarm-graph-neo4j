package org.dswarm.graph.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class Neo4jGDMWDataModelVersionHandler extends Neo4jGDMBaseVersionHandler {

	private static final Logger LOG = LoggerFactory.getLogger(Neo4jGDMWDataModelVersionHandler.class);

	private final String dataModelURI;

	public Neo4jGDMWDataModelVersionHandler(final String dataModelURIArg) {

		super();

		dataModelURI = dataModelURIArg;
	}

	@Override
	public void setLatestVersion(final String dataModelURI) throws DMPGraphException {

		final String finalDataModelURI;

		if (dataModelURI != null) {

			finalDataModelURI = dataModelURI;
		} else {

			finalDataModelURI = this.dataModelURI;
		}

		super.setLatestVersion(finalDataModelURI);
	}

	@Override
	protected int retrieveLatestVersion() {

		int latestVersion = 1;

		final IndexHits<Node> hits = processor.getResourcesIndex().get(GraphStatics.URI, dataModelURI);

		if (hits != null && hits.hasNext()) {

			final Node dataModelNode = hits.next();
			final Integer latestVersionFromDB = (Integer) dataModelNode.getProperty(VersioningStatics.LATEST_VERSION_PROPERTY, null);

			if (latestVersionFromDB != null) {

				latestVersion = latestVersionFromDB;
			}
		}

		if (hits != null) {

			hits.close();
		}

		return latestVersion;
	}

	@Override
	public void updateLatestVersion() throws DMPGraphException {

		try (final Transaction tx = processor.getDatabase().beginTx()) {

			final IndexHits<Node> hits = processor.getResourcesIndex().get(GraphStatics.URI, dataModelURI);

			if (hits != null && hits.hasNext()) {

				final Node dataModelNode = hits.next();
				dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, latestVersion);
			}

			if (hits != null) {

				hits.close();
			}

			tx.success();
		} catch (final Exception e) {

			final String message = "couldn't update latest version";

			Neo4jGDMWDataModelVersionHandler.LOG.error(message, e);
			Neo4jGDMWDataModelVersionHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}
}
