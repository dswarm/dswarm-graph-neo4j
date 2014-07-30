package org.dswarm.graph.delta.util;

import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.Traversal;

/**
 * Created by tgaengler on 30/07/14.
 */
public class PathPrinter implements Traversal.PathDescriptor<Path> {

	private final static String	PREFIX			= "<-";
	private final static String	INTERMEDIATE	= "--";
	private final static String	SUFFIX			= "-->";

	@Override
	public String nodeRepresentation(Path path, Node node) {

		final String nodeTypeString = (String) node.getProperty(GraphStatics.NODETYPE_PROPERTY, null);
		final NodeType nodeType = NodeType.getByName(nodeTypeString);
		final StringBuilder sb = new StringBuilder();
		sb.append("(").append(node.getId()).append(":type='").append(nodeType).append("',");

		switch (nodeType) {

			case Resource:
			case TypeResource:

				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				sb.append("uri='").append(uri);

				break;
			case Literal:

				final String value = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);
				sb.append("value='").append(value);

				break;
		}

		sb.append("')");

		return sb.toString();
	}

	@Override
	public String relationshipRepresentation(Path path, Node from, Relationship relationship) {

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
		sb.append(prefix).append("[").append(relationship.getId()).append(":").append(relationship.getType().name()).append(",");

		final Long order = (Long) relationship.getProperty(GraphStatics.ORDER_PROPERTY, null);

		if (order != null) {

			sb.append("order='").append(order).append("',");
		}

		final Long index = (Long) relationship.getProperty(GraphStatics.INDEX_PROPERTY, null);
		sb.append("index='").append(index).append("']").append(suffix);

		return sb.toString();
	}
}
