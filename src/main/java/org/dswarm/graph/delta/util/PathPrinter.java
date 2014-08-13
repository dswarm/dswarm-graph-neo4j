package org.dswarm.graph.delta.util;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.Traversal;

/**
 * @author tgaengler
 */
public class PathPrinter implements Traversal.PathDescriptor<Path> {

	private final static String	PREFIX			= "<-";
	private final static String	INTERMEDIATE	= "--";
	private final static String	SUFFIX			= "-->";

	@Override
	public String nodeRepresentation(final Path path, final Node node) {

		return GraphDBUtil.printNode(node);
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
		sb.append(prefix).append(GraphDBUtil.printRelationship(relationship)).append(suffix);

		return sb.toString();
	}
}
