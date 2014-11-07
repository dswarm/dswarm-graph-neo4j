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
