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
package org.dswarm.graph.delta.util;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public class PathPrinter implements Paths.PathDescriptor<Path> {

	private static final Logger LOG = LoggerFactory.getLogger(PathPrinter.class);

	private final static String PREFIX       = "<-";
	private final static String INTERMEDIATE = "--";
	private final static String SUFFIX       = "-->";

	@Override
	public String nodeRepresentation(final Path path, final Node node) {

		try {

			return GraphDBPrintUtil.printNode(node);
		} catch (final DMPGraphException e) {

			final String msg = "couldn't print path";

			LOG.error(msg, e);

			throw new RuntimeException(msg, e);
		}
	}

	@Override
	public String relationshipRepresentation(final Path path, final Node from, final Relationship relationship) {

		final String prefix;
		final String suffix;

		if (from.equals(relationship.getEndNode())) {

			prefix = PREFIX;
			suffix = INTERMEDIATE;
		} else {

			suffix = SUFFIX;
			prefix = INTERMEDIATE;
		}

		final StringBuilder sb = new StringBuilder();
		sb.append(prefix).append(GraphDBPrintUtil.printRelationship(relationship)).append(suffix);

		return sb.toString();
	}
}
