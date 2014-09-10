package org.dswarm.graph.gdm.parse;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.NotImplementedException;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.gdm.read.PropertyGraphGDMReader;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.Range;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * TODO: maybe we should add a general type for (bibliographic) resources (to easily identify the boundaries of the resources)
 *
 * @author tgaengler
 */
public abstract class Neo4jBaseGDMUpdateHandler extends CommonNeo4jGDMHandler implements GDMUpdateHandler {

	private static final Logger				LOG							= LoggerFactory.getLogger(Neo4jBaseGDMUpdateHandler.class);

	protected final PropertyGraphGDMReader	propertyGraphGDMReader		= new PropertyGraphGDMReader();

	public Neo4jBaseGDMUpdateHandler(final GraphDatabaseService database) throws DMPGraphException {

		super(database);
	}

	@Override
	public void handleStatement(final String stmtUUID, final Resource resource, final long index, final long order) throws DMPGraphException {

		try {

			final Relationship rel = getRelationship(stmtUUID);
			final Node subject = rel.getStartNode();
			final Node object = rel.getEndNode();
			final Statement stmt = propertyGraphGDMReader.readStatement(rel);
			addBNode(stmt.getSubject(), subject);
			addBNode(stmt.getObject(), object);

			// reset stmt uuid, so that a new stmt uuid will be assigned when relationship will be added
			stmt.setUUID(null);
			// set actual order of the stmt
			stmt.setOrder(order);
			final String predicate = stmt.getPredicate().getUri();

			// TODO: shall we include some more qualified attributes into hash generation, e.g., index, valid from, or will the
			// index
			// be update with the new stmt (?)
			final String hash = generateStatementHash(subject, predicate, object, stmt.getSubject().getType(), stmt.getObject().getType());

			addRelationship(subject, object, resource.getUri(), resource, stmt, index, hash);

			totalTriples++;
		} catch (final DMPGraphException e) {

			throw e;
		} catch (final Exception e) {

			final String message = "couldn't handle statement successfully";

			tx.failure();
			tx.close();

			Neo4jBaseGDMUpdateHandler.LOG.error(message, e);
			Neo4jBaseGDMUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public void deprecateStatement(long index) {

		throw new NotImplementedException();
	}

	@Override
	public org.dswarm.graph.json.Node deprecateStatement(final String uuid) throws DMPGraphException {

		try {

			final Relationship rel = getRelationship(uuid);

			rel.setProperty(VersioningStatics.VALID_TO_PROPERTY, versionHandler.getLatestVersion());

			final org.dswarm.graph.json.Node subjectGDMNode = propertyGraphGDMReader.readObject(rel.getStartNode());
			final org.dswarm.graph.json.Node objectGDMNode = propertyGraphGDMReader.readObject(rel.getEndNode());
			addBNode(subjectGDMNode, rel.getStartNode());
			addBNode(objectGDMNode, rel.getEndNode());

			return subjectGDMNode;
		} catch (final DMPGraphException e) {

			throw e;
		} catch (final Exception e) {

			final String message = "couldn't deprecate statement successfully";

			tx.failure();
			tx.close();

			Neo4jBaseGDMUpdateHandler.LOG.error(message, e);
			Neo4jBaseGDMUpdateHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	protected abstract Relationship getRelationship(final String uuid);

	private void addBNode(final org.dswarm.graph.json.Node gdmNode, final Node node) {

		switch (gdmNode.getType()) {

			case BNode:

				bnodes.put("" + gdmNode.getId(), node);

				break;
		}
	}
}
