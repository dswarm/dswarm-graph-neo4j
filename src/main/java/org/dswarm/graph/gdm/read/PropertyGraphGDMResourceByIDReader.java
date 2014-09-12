package org.dswarm.graph.gdm.read;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.AttributePath;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class PropertyGraphGDMResourceByIDReader extends PropertyGraphGDMResourceReader {

	private static final Logger	LOG	= LoggerFactory.getLogger(PropertyGraphGDMResourceByIDReader.class);

	private final String		recordId;
	private String recordURI;
	private final AttributePath	recordIdentifierAP;

	public PropertyGraphGDMResourceByIDReader(final String recordIdArg, final AttributePath recordIdentifierAPArg, final String dataModelUri,
			final GraphDatabaseService database) throws DMPGraphException {

		super(dataModelUri, database);

		recordId = recordIdArg;
		recordIdentifierAP = recordIdentifierAPArg;
		determineRecordUri();
	}

	@Override
	protected Node getResourceNode() {

		if (recordURI == null) {

			LOG.debug("couldn't a find a resource node to start traversal");

			return null;
		}

		final PropertyGraphGDMResourceByURIReader uriReader = new PropertyGraphGDMResourceByURIReader(recordURI, dataModelUri, database);

		return uriReader.getResourceNode();
	}

	private void determineRecordUri() throws DMPGraphException {

		recordURI = GraphDBUtil.determineRecordUri(recordId, recordIdentifierAP, dataModelUri, database);
	}
}
