package org.dswarm.graph.delta.util;

import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * Created by tgaengler on 29/07/14.
 */
public final class GraphDBUtil {

	/**
	 * note: should be run in transaction scope
	 * 
	 * @param graphDB
	 * @param resourceURI
	 * @return
	 */
	public static Node getResourceNode(final GraphDatabaseService graphDB, final String resourceURI) {

		final Index<Node> resources = graphDB.index().forNodes("resources");

		if (resources == null) {

			return null;
		}

		final IndexHits<Node> hits = resources.get(GraphStatics.URI, resourceURI);

		if (hits == null || !hits.hasNext()) {

			return null;
		}

		return hits.next();
	}

	public static void printNodes(final GraphDatabaseService graphDB) {

		Transaction tx = graphDB.beginTx();

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
		tx.close();
	}

	public static void printRelationships(final GraphDatabaseService graphDB) {

		Transaction tx = graphDB.beginTx();

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
		tx.close();
	}

}
