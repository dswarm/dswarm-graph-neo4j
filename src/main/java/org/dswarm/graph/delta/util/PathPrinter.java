package org.dswarm.graph.delta.util;

import org.dswarm.graph.NodeType;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.Label;
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
	public String nodeRepresentation(final Path path, final Node node) {

		final String nodeTypeString = (String) node.getProperty(GraphStatics.NODETYPE_PROPERTY, null);
		final NodeType nodeType = NodeType.getByName(nodeTypeString);
		final StringBuilder sb = new StringBuilder();
		sb.append("(").append(node.getId()).append(":type='").append(nodeType).append("',");

		final String labels = getLabels(node);

		sb.append("label='").append(labels);

		switch (nodeType) {

			case Resource:
			case TypeResource:

				sb.append("',");

				final String uri = (String) node.getProperty(GraphStatics.URI_PROPERTY, null);
				sb.append("uri='").append(uri);

				break;
			case Literal:

				sb.append("',");

				final String value = (String) node.getProperty(GraphStatics.VALUE_PROPERTY, null);
				sb.append("value='").append(value);

				break;
		}

		sb.append("'");

		final Boolean matched = (Boolean) node.getProperty("MATCHED", null);

		if(matched != null) {

			sb.append(",matched='").append(matched).append("'");
		}

		final String deltaState = (String) node.getProperty("DELTA_STATE", null);

		if(deltaState != null) {

			sb.append(",delta_state='").append(deltaState).append("'");
		}

		sb.append(")");

		return sb.toString();
	}

	private String getLabels(final Node node) {

		final StringBuilder sb2 = new StringBuilder();

		for(final Label label : node.getLabels()) {

			sb2.append(label.name()).append(",");
		}

		final String tempLabels = sb2.toString();

		return tempLabels.substring(0, tempLabels.length() - 1);
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
		sb.append(prefix).append("[").append(relationship.getId()).append(":").append(relationship.getType().name()).append(",");

		final Long order = (Long) relationship.getProperty(GraphStatics.ORDER_PROPERTY, null);

		if (order != null) {

			sb.append("order='").append(order).append("',");
		}

		final Long index = (Long) relationship.getProperty(GraphStatics.INDEX_PROPERTY, null);
		sb.append("index='").append(index);

		sb.append("'");

		final Boolean matched = (Boolean) relationship.getProperty("MATCHED", null);

		if(matched != null) {

			sb.append(",matched='").append(matched).append("'");
		}

		final String deltaState = (String) relationship.getProperty("DELTA_STATE", null);

		if(deltaState != null) {

			sb.append(",delta_state='").append(deltaState).append("'");
		}

		sb.append("]").append(suffix);

		return sb.toString();
	}
}
