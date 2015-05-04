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
package org.dswarm.graph.gdm.parse;

import java.util.Map;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.NodeType;
import org.dswarm.graph.TransactionalNeo4jProcessor;
import org.dswarm.graph.gdm.GDMNeo4jProcessor;
import org.dswarm.graph.gdm.read.PropertyGraphGDMReaderHelper;
import org.dswarm.graph.gdm.utils.NodeTypeUtils;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.model.StatementBuilder;
import org.dswarm.graph.parse.BaseNeo4jHandler;
import org.dswarm.graph.parse.Neo4jHandler;
import org.dswarm.graph.parse.TransactionalNeo4jHandler;

/**
 * @author tgaengler
 */
public abstract class GDMNeo4jHandler implements GDMHandler, GDMUpdateHandler {

	private static final Logger				LOG						= LoggerFactory.getLogger(GDMNeo4jHandler.class);

	protected final BaseNeo4jHandler		handler;
	protected final GDMNeo4jProcessor		processor;

	protected final PropertyGraphGDMReaderHelper propertyGraphGDMReaderHelper = new PropertyGraphGDMReaderHelper();

	public GDMNeo4jHandler(final BaseNeo4jHandler handlerArg, final GDMNeo4jProcessor processorArg) throws DMPGraphException {

		handler = handlerArg;
		processor = processorArg;
	}

	@Override
	public Neo4jHandler getHandler() {

		return handler;
	}

	@Override public GraphDatabaseService getDatabase() {

		return ((TransactionalNeo4jHandler) handler).getDatabase();
	}

	@Override
	public void handleStatement(final Statement st, final long resourceHash, final long index) throws DMPGraphException {

		final StatementBuilder sb = new StatementBuilder();

		final org.dswarm.graph.json.Node subject = st.getSubject();
		final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeType(Optional.of(subject));
		sb.setOptionalSubjectNodeType(optionalSubjectNodeType);
		processor.determineNode(subject, sb, true);

		final org.dswarm.graph.json.Predicate predicate = st.getPredicate();
		final String predicateName = predicate.getUri();
		sb.setOptionalPredicateURI(Optional.fromNullable(predicateName));

		final org.dswarm.graph.json.Node object = st.getObject();
		final Optional<NodeType> optionalObjectNodeType = NodeTypeUtils.getNodeType(Optional.of(object));
		sb.setOptionalObjectNodeType(optionalObjectNodeType);
		processor.determineNode(object, sb, false);

		final Optional<String> optionalStatementUUID = Optional.fromNullable(st.getUUID());
		sb.setOptionalStatementUUID(optionalStatementUUID);

		sb.setOptionalResourceHash(Optional.of(resourceHash));

		final Map<String, Object> qualifiedAttributes = processor.getQualifiedAttributes(st);
		qualifiedAttributes.put(GraphStatics.INDEX_PROPERTY, index);
		sb.setOptionalQualifiedAttributes(Optional.of(qualifiedAttributes));

		final org.dswarm.graph.model.Statement statement = sb.build();

		handler.handleStatement(statement);
	}

	@Override public long getCountedStatements() {
		return handler.getCountedStatements();
	}

	@Override public int getRelationshipsAdded() {
		return handler.getRelationshipsAdded();
	}

	@Override public int getNodesAdded() {
		return handler.getNodesAdded();
	}

	@Override public int getCountedLiterals() {
		return handler.getCountedLiterals();
	}

	@Override public NamespaceIndex getNamespaceIndex() {

		return processor.getProcessor().getNamespaceIndex();
	}

	/**
	 * TODO: refactor this to BaseNeo4jHandler
	 *
	 * @param stmtUUID
	 * @param resourceHash
	 * @param index
	 * @param order
	 * @throws DMPGraphException
	 */
	@Override
	public void handleStatement(final String stmtUUID, final long resourceHash, final long index, final long order) throws DMPGraphException {

		((TransactionalNeo4jProcessor) handler.getProcessor()).ensureRunningTx();

		try {

			final Optional<Relationship> optionalRel = ((TransactionalNeo4jProcessor) handler.getProcessor()).getRelationshipFromStatementIndex(
					stmtUUID);

			if(!optionalRel.isPresent()) {

				GDMNeo4jHandler.LOG.error("couldn't find statement with the uuid '{}' in the database", stmtUUID);
			}

			final Relationship rel = optionalRel.get();

			final Node subject = rel.getStartNode();
			final Node object = rel.getEndNode();
			final Statement stmt = propertyGraphGDMReaderHelper.readStatement(rel);
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
			final long hash = processor.generateStatementHash(subject, predicate, object, stmt.getSubject().getType(), stmt.getObject().getType());
			final Optional<NodeType> optionalSubjectNodeType = NodeTypeUtils.getNodeType(Optional.fromNullable(stmt.getSubject()));
			final Optional<Long> optionalSubjectHash;

			if (stmt.getSubject().getType().equals(org.dswarm.graph.json.NodeType.Resource)) {

				final ResourceNode subjectNode = (ResourceNode) stmt.getSubject();
				final String subjectURI = processor.getProcessor().createPrefixedURI(subjectNode.getUri());
				final String dataModelURI = processor.getProcessor().createPrefixedURI(subjectNode.getDataModel());

				final long resourceUriDataModelUriHash = processor.getProcessor().generateResourceHash(subjectURI, Optional.fromNullable(dataModelURI));

				optionalSubjectHash = Optional.of(resourceUriDataModelUriHash);
			} else {

				optionalSubjectHash = Optional.absent();
			}

			final Optional<Long> optionalResourceHash = processor.determineResourceHash(stmt.getSubject(), resourceHash);
			final Map<String, Object> qualifiedAttributes = processor.getQualifiedAttributes(stmt);
			qualifiedAttributes.put(GraphStatics.INDEX_PROPERTY, index);
			qualifiedAttributes.put(GraphStatics.ORDER_PROPERTY, order);
			final Optional<Map<String, Object>> optionalQualifiedAttributes = Optional.of(qualifiedAttributes);

			handler.addRelationship(subject, predicate, object, optionalSubjectNodeType, optionalSubjectHash, Optional.<String> absent(),
					optionalResourceHash, optionalQualifiedAttributes, hash);
		} catch (final DMPGraphException e) {

			throw e;
		} catch (final Exception e) {

			final String message = "couldn't handle statement successfully";

			((TransactionalNeo4jProcessor) handler.getProcessor()).failTx();

			GDMNeo4jHandler.LOG.error(message, e);
			GDMNeo4jHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	@Override
	public org.dswarm.graph.json.Node deprecateStatement(final String uuid) throws DMPGraphException {

		((TransactionalNeo4jProcessor) handler.getProcessor()).ensureRunningTx();

		try {

			final Relationship rel = handler.deprecateStatement(uuid);

			final org.dswarm.graph.json.Node subjectGDMNode = propertyGraphGDMReaderHelper.readObject(rel.getStartNode());
			final org.dswarm.graph.json.Node objectGDMNode = propertyGraphGDMReaderHelper.readObject(rel.getEndNode());

			addBNode(subjectGDMNode, rel.getStartNode());
			addBNode(objectGDMNode, rel.getEndNode());

			return subjectGDMNode;
		} catch (final DMPGraphException e) {

			throw e;
		} catch (final Exception e) {

			final String message = "couldn't deprecate statement successfully";

			((TransactionalNeo4jProcessor) handler.getProcessor()).failTx();

			GDMNeo4jHandler.LOG.error(message, e);
			GDMNeo4jHandler.LOG.debug("couldn't finish write TX successfully");

			throw new DMPGraphException(message);
		}
	}

	private void addBNode(final org.dswarm.graph.json.Node gdmNode, final Node node) throws DMPGraphException {

		final Optional<NodeType> optionalNodeType = NodeTypeUtils.getNodeType(Optional.of(gdmNode));

		final Optional<String> optionalNodeId;

		if (gdmNode.getId() != null) {

			optionalNodeId = Optional.of("" + gdmNode.getId());
		} else {

			optionalNodeId = Optional.absent();
		}

		handler.addBNode(optionalNodeId, optionalNodeType, node);
	}
}
