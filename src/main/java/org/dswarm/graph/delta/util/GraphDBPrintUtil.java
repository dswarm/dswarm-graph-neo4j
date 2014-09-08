package org.dswarm.graph.delta.util;

import org.dswarm.graph.NodeType;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public final class GraphDBPrintUtil {

	private static final Logger	LOG	= LoggerFactory.getLogger(GraphDBPrintUtil.class);

	public static void printRelationships(final GraphDatabaseService graphDB) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Relationship> relationships = GlobalGraphOperations.at(graphDB).getAllRelationships();

			for (final Relationship relationship : relationships) {

				final RelationshipType type = relationship.getType();

				System.out.println("relationship = '" + relationship.getId() + "' :: relationship type = '" + type.name());

				final Iterable<String> propertyKeys = relationship.getPropertyKeys();

				for (final String propertyKey : propertyKeys) {

					final Object value = relationship.getProperty(propertyKey);

					System.out.println("relationship = '" + relationship.getId() + "' :: key = '" + propertyKey + "' :: value = '" + value + "'");
				}
			}

			tx.success();
		} catch (final Exception e) {

			GraphDBPrintUtil.LOG.error("couldn't print relationships", e);

			tx.failure();
		} finally {

			tx.close();
		}
	}

	public static void printDeltaRelationships(final GraphDatabaseService graphDB) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Relationship> relationships = GlobalGraphOperations.at(graphDB).getAllRelationships();

			for (final Relationship relationship : relationships) {

				final String sb = printDeltaRelationship(relationship);

				System.out.println(sb);
			}

			tx.success();
		} catch (final Exception e) {

			GraphDBPrintUtil.LOG.error("couldn't print relationships", e);

			tx.failure();
		} finally {

			tx.close();
		}
	}

	public static void printNodes(final GraphDatabaseService graphDB) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Node> nodes = GlobalGraphOperations.at(graphDB).getAllNodes();

			for (final Node node : nodes) {

				final Iterable<Label> labels = node.getLabels();

				for (final Label label : labels) {

					System.out.println("node = '" + node.getId() + "' :: label = '" + label.name());
				}

				final Iterable<String> propertyKeys = node.getPropertyKeys();

				for (final String propertyKey : propertyKeys) {

					final Object value = node.getProperty(propertyKey);

					System.out.println("node = '" + node.getId() + "' :: key = '" + propertyKey + "' :: value = '" + value + "'");
				}
			}

			tx.success();
		} catch (final Exception e) {

			GraphDBPrintUtil.LOG.error("couldn't print nodes", e);

			tx.failure();
		} finally {

			tx.close();
		}
	}

	public static String printDeltaRelationship(final Relationship relationship) {

		final Long index = (Long) relationship.getProperty(GraphStatics.INDEX_PROPERTY, null);
		final String startNodeString = printNode(relationship.getStartNode());
		final String relString = printRelationship(relationship);
		final String endNodeString = printNode(relationship.getEndNode());

		final StringBuilder sb = new StringBuilder();

		sb.append(index).append(" : ").append(startNodeString).append("-").append(relString).append("->").append(endNodeString);

		return sb.toString();
	}

	public static String printNode(final Node node) {

		final String nodeTypeString = (String) node.getProperty(GraphStatics.NODETYPE_PROPERTY, null);
		final NodeType nodeType = NodeType.getByName(nodeTypeString);
		final StringBuilder sb = new StringBuilder();
		sb.append("(").append(node.getId()).append(":type='").append(nodeType).append("',");

		final String labels = GraphDBUtil.getLabels(node);

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

		final Boolean matched = (Boolean) node.getProperty(DeltaStatics.MATCHED_PROPERTY, null);

		if (matched != null) {

			sb.append(",matched='").append(matched).append("'");
		}

		final String deltaState = (String) node.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);

		if (deltaState != null) {

			sb.append(",delta_state='").append(deltaState).append("'");
		}

		sb.append(")");

		return sb.toString();
	}

	public static String printRelationship(final Relationship relationship) {

		final StringBuilder sb = new StringBuilder();
		sb.append("[").append(relationship.getId()).append(":").append(relationship.getType().name()).append(",");

		final Long order = (Long) relationship.getProperty(GraphStatics.ORDER_PROPERTY, null);

		if (order != null) {

			sb.append("order='").append(order).append("',");
		}

		final Long index = (Long) relationship.getProperty(GraphStatics.INDEX_PROPERTY, null);
		sb.append("index='").append(index);

		sb.append("'");

		final Boolean matched = (Boolean) relationship.getProperty(DeltaStatics.MATCHED_PROPERTY, null);

		if (matched != null) {

			sb.append(",matched='").append(matched).append("'");
		}

		final String deltaState = (String) relationship.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);

		if (deltaState != null) {

			sb.append(",delta_state='").append(deltaState).append("'");
		}

		sb.append("]");

		return sb.toString();
	}

	public static void printPaths(final GraphDatabaseService graphDB, final String resourceURI) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Path> paths = GraphDBUtil.getResourcePaths(graphDB, resourceURI);
			printPaths(paths);

			tx.success();
		} catch (final Exception e) {

			GraphDBPrintUtil.LOG.error("couldn't print paths", e);

			tx.failure();
		} finally {

			tx.close();
		}
	}

	/**
	 * note: should be run in transaction scope
	 *
	 * @param paths
	 */
	public static void printPaths(final Iterable<Path> paths) {

		final Traversal.PathDescriptor<Path> pathPrinter = new PathPrinter();

		for (final Path path : paths) {

			final String pathString = Traversal.pathToString(path, pathPrinter);

			System.out.println(pathString);
		}
	}

	public static void printEntityPaths(final GraphDatabaseService graphDB, final long nodeId) {

		final Transaction tx = graphDB.beginTx();

		try {

			final Iterable<Path> paths = GraphDBUtil.getEntityPaths(graphDB, nodeId);
			printPaths(paths);

			tx.success();
		} catch (final Exception e) {

			GraphDBPrintUtil.LOG.error("couldn't print entity paths", e);

			tx.failure();
		} finally {

			tx.close();
		}
	}
}
