package org.dswarm.graph.gdm.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.DataModelNeo4jProcessor;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class DataModelGDMNeo4jVersionHandler extends GDMNeo4jVersionHandler {

	private static final Logger	LOG	= LoggerFactory.getLogger(DataModelGDMNeo4jVersionHandler.class);

	public DataModelGDMNeo4jVersionHandler(final GDMNeo4jProcessor processorArg) throws DMPGraphException {

		super(processorArg);

		processor.getProcessor().ensureRunningTx();

		try {

			init();
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			final String message = "couldn't init version handler successfully";

			DataModelGDMNeo4jVersionHandler.LOG.error(message, e);
			DataModelGDMNeo4jVersionHandler.LOG.debug("couldn't finish TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void setLatestVersion(final String dataModelURI) throws DMPGraphException {

		final String finalDataModelURI;

		if (dataModelURI != null) {

			finalDataModelURI = dataModelURI;
		} else {

			finalDataModelURI = ((DataModelNeo4jProcessor) processor.getProcessor()).getDataModelURI();
		}

		super.setLatestVersion(finalDataModelURI);
	}

	@Override
	protected int retrieveLatestVersion() {

		int latestVersion = 0;

		final IndexHits<Node> hits = processor.getProcessor().getResourcesIndex()
				.get(GraphStatics.URI, ((DataModelNeo4jProcessor) processor.getProcessor()).getDataModelURI());

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

		processor.getProcessor().ensureRunningTx();

		try {

			final IndexHits<Node> hits = processor.getProcessor().getResourcesIndex()
					.get(GraphStatics.URI, ((DataModelNeo4jProcessor) processor.getProcessor()).getDataModelURI());

			if (hits != null && hits.hasNext()) {

				final Node dataModelNode = hits.next();
				dataModelNode.setProperty(VersioningStatics.LATEST_VERSION_PROPERTY, latestVersion);
			}

			if (hits != null) {

				hits.close();
			}
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			final String message = "couldn't update latest version";

			DataModelGDMNeo4jVersionHandler.LOG.error(message, e);
			DataModelGDMNeo4jVersionHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}
}
