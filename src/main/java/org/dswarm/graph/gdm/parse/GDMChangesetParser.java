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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.delta.Changeset;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.delta.util.GraphDBPrintUtil;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public class GDMChangesetParser implements GDMUpdateParser {

	private static final Logger LOG = LoggerFactory.getLogger(GDMChangesetParser.class);

	private       GDMUpdateHandler     gdmHandler;
	private final Changeset            changeset;
	private final long                 existingResourceHash;
	private final GraphDatabaseService existingResourceDB;
	private final GraphDatabaseService newResourceDB;

	public GDMChangesetParser(final Changeset changesetArg, final long existingResourceHashArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg) {

		changeset = changesetArg;
		existingResourceHash = existingResourceHashArg;
		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;
	}

	@Override
	public void setGDMHandler(final GDMUpdateHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public void parse() throws DMPGraphException {

		if (changeset == null || existingResourceDB == null || newResourceDB == null) {

			LOG.debug("there is no change set or resource working sets");

			gdmHandler.getHandler().closeTransaction();

			return;
		}

		LOG.debug("start processing changeset");

		// 0. fetch latest version from data model node (+ increase this value + update resource node (maybe at the end))

		// 1. compare existing resource DB statements with new resource DB statements, i.e. write/follow statements ordered by
		// index property (it might be good to also have the relationship in the permanent DB that is related to this index
		// available)
		final Transaction newResourceDBTX = newResourceDB.beginTx();
		final Transaction existingResourceDBTX = existingResourceDB.beginTx();

		try {

			final Iterable<Relationship> existingRelationships = GlobalGraphOperations.at(existingResourceDB).getAllRelationships();
			final Iterator<Relationship> existingRelationshipsIter = existingRelationships.iterator();
			final Iterable<Relationship> newRelationships = GlobalGraphOperations.at(newResourceDB).getAllRelationships();
			final Iterator<Relationship> newRelationshipsIter = newRelationships.iterator();

			final Set<String> alreadyAddedStatementUUIDs = new HashSet<>();
			final Set<String> alreadyDeletedStatementUUIDs = new HashSet<>();
			final Set<String> alreadyModifiedExistingStatementUUIDs = new HashSet<>();
			final Set<String> alreadyModifiedNewStatementUUIDs = new HashSet<>();

			long index = 1;
			Relationship existingRelationship = existingRelationshipsIter.next();
			Relationship newRelationship = null;

			do {

				final Long indexFromDB;

				if (existingRelationship != null) {

					indexFromDB = (Long) existingRelationship.getProperty(GraphStatics.INDEX_PROPERTY, null);
				} else if (newRelationship != null) {

					indexFromDB = (Long) newRelationship.getProperty(GraphStatics.INDEX_PROPERTY, null);
				} else {

					indexFromDB = null;
				}

				if (indexFromDB == null) {

					// TODO: we should probably throw an exception instead

					break;
				}

				if (!newRelationshipsIter.hasNext()) {

					// TODO: we should probably throw an exception instead

					break;
				}

				final DeltaState existingRelDeltaState = getDeltaState(existingRelationship);

				final DeltaState finalDeltaState;
				boolean increaseExistingRelationship = true;

				if (existingRelDeltaState == null || DeltaState.ExactMatch.equals(existingRelDeltaState)) {

					newRelationship = getNewRel(newRelationshipsIter, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs,
							alreadyModifiedNewStatementUUIDs);

					if (newRelationship == null) {

						break;
					}

					finalDeltaState = getDeltaState(newRelationship);
				} else {

					finalDeltaState = existingRelDeltaState;
					final boolean stmtAlreadyProcessed = checkStmt(existingRelationship, finalDeltaState, alreadyAddedStatementUUIDs,
							alreadyDeletedStatementUUIDs, alreadyModifiedExistingStatementUUIDs);

					if (stmtAlreadyProcessed) {

						// skip already processed existing stmt

						existingRelationship = increaseRelationship(existingRelationshipsIter);

						continue;
					}
				}

				switch (finalDeltaState) {

					case ADDITION:

						final String newResourceStmtUUID = (String) newRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
						final Statement addedStatement = changeset.getAdditions().get(newResourceStmtUUID);

						// retrieve start node via subject identifier (?) - start node must be a resource node (i.e., we could probably verify this requirement)
						// retrieve the resource identifier from the existing resource and replace the subject of the to-be-added statement
						if (ResourceNode.class.isInstance(addedStatement.getSubject())) {

							final ResourceNode subject = (ResourceNode) addedStatement.getSubject();
							final String subjectURI = subject.getUri();
							final String prefixedSubjectURI = gdmHandler.getHandler().getProcessor().createPrefixedURI(subjectURI);
							final Optional<String> optionalDataModelURI;

							if (subject.getDataModel() != null) {

								optionalDataModelURI = Optional
										.fromNullable(gdmHandler.getHandler().getProcessor().createPrefixedURI(subject.getDataModel()));
							} else {

								optionalDataModelURI = Optional.absent();
							}

							final long subjectHash = gdmHandler.getHandler().getProcessor().generateResourceHash(prefixedSubjectURI, optionalDataModelURI);

							if (existingResourceHash != subjectHash) {

								// TODO: do something, e.g., replace subject of the to-be-added statement;

								break;
							}
						}

						final String addedStmtUUID = addedStatement.getUUID();

						gdmHandler.handleStatement(addedStatement, existingResourceHash, index);
						alreadyAddedStatementUUIDs.add(addedStmtUUID);

						// simply increase the index?
						index++;

						increaseExistingRelationship = false;

						break;
					case DELETION:

						final String existingResourceStmtUUID = (String) existingRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
						// note: we don't need to retrieve the stmt from the changeset, we just need the uuid of it
						// final Statement deletedStatement = changeset.getDeletions().get(existingResourceStmtUUID);

						// utilise statement uuid from existing statement to deprecate it
						gdmHandler.deprecateStatement(existingResourceStmtUUID);
						alreadyDeletedStatementUUIDs.add(existingResourceStmtUUID);

						break;
					case MODIFICATION:

						final Long modifiedNodeId = changeset.getModifications().get(existingRelationship.getEndNode().getId());
						final Statement modifiedStatement = changeset.getNewModifiedStatements().get(modifiedNodeId);

						final Statement finalModifiedStatement;
						final String existingModifiedStmtUUID;

						if (modifiedStatement != null) {

							finalModifiedStatement = modifiedStatement;
							existingModifiedStmtUUID = (String) existingRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
						} else {

							final Long newModifiedNodeId = newRelationship.getEndNode().getId();

							finalModifiedStatement = changeset.getNewModifiedStatements().get(newModifiedNodeId);

							Long existingModifiedNodeId = null;

							for (final Map.Entry<Long, Long> modificationsEntry : changeset.getModifications().entrySet()) {

								if (modificationsEntry.getValue().equals(newModifiedNodeId)) {

									existingModifiedNodeId = modificationsEntry.getKey();

									break;
								}
							}

							if (existingModifiedNodeId == null) {

								// TODO: do something, e.g., throw an exception

								break;
							}

							final Statement existingModifiedStatement = changeset.getExistingModifiedStatements().get(existingModifiedNodeId);
							existingModifiedStmtUUID = existingModifiedStatement.getUUID();

							increaseExistingRelationship = false;
						}

						if (finalModifiedStatement == null) {

							// TODO: do something, e.g., throw an exception

							break;
						}

						// utilise statement uuid from existing statenent to deprecate it + to get modification (?)
						final Node subject = gdmHandler.deprecateStatement(existingModifiedStmtUUID);
						alreadyModifiedExistingStatementUUIDs.add(existingModifiedStmtUUID);

						// take subject from existing resource to append the statement on the correct position
						finalModifiedStatement.setSubject(subject);

						gdmHandler.handleStatement(finalModifiedStatement, existingResourceHash, index);
						alreadyModifiedNewStatementUUIDs.add(finalModifiedStatement.getUUID());

						index++;

						break;
				}

				if (!finalDeltaState.equals(DeltaState.ExactMatch)) {

					if (increaseExistingRelationship) {

						existingRelationship = increaseRelationship(existingRelationshipsIter);
					}

					continue;
				}

				if (newRelationship == null) {

					newRelationship = increaseRelationship(newRelationshipsIter);
				}

				final String existingRelationshipPrint = GraphDBPrintUtil.printRelationship(existingRelationship);
				final String newRelationshipPrint = GraphDBPrintUtil.printRelationship(newRelationship);

				if (!(existingRelationshipPrint.equals(newRelationshipPrint) && index == indexFromDB)) {

					// note: we don't really know how equal/unequal the statements are at this moment, so it's better to compare them more in detail (? - once again?) - we could also hold a map of exact matched statements

					// deprecate old statement and write it as new statement with a different index
					final String existingStmtUUID = (String) existingRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
					final Long newStmtOrder = (Long) newRelationship.getProperty(GraphStatics.ORDER_PROPERTY, null);

					final long finalNewStmtOrder;

					if (newStmtOrder != null) {

						finalNewStmtOrder = newStmtOrder;
					} else {

						finalNewStmtOrder = (long) 1;
					}

					gdmHandler.deprecateStatement(existingStmtUUID);

					gdmHandler.handleStatement(existingStmtUUID, existingResourceHash, index, finalNewStmtOrder);
				}

				index++;

				existingRelationship = increaseRelationship(existingRelationshipsIter);

			} while (newRelationshipsIter.hasNext() || existingRelationshipsIter.hasNext());

			// System.out.println("index = '" + (index -1) + "'");

			newResourceDBTX.success();
			existingResourceDBTX.success();
		} catch (final Exception e) {

			GDMChangesetParser.LOG.error("couldn't write changeset successfully to graph DB", e);

			newResourceDBTX.failure();
			existingResourceDBTX.failure();
		} finally {

			newResourceDBTX.close();
			existingResourceDBTX.close();
		}

		// 1.1 if a statement was added or deleted or the printed version doesn't equal, rewrite all following statements
		// afterwards and deprecated the existing ones (i.e. update their valid to value)
		// for added + modified statements utilise the current version for valid from

		LOG.debug("finished processing changeset");
	}

	private DeltaState getDeltaState(final Relationship relationship) {

		if (relationship == null) {

			return null;
		}

		final String deltaStateString = (String) relationship.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);

		return DeltaState.getByName(deltaStateString);
	}

	private boolean checkStmt(final Relationship rel, final DeltaState deltaState, final Set<String> alreadyAddedStatementUUIDs,
			final Set<String> alreadyDeletedStatementUUIDs, final Set<String> alreadyModifiedStatementUUIDs) {

		if (rel == null) {

			return false;
		}

		final String newStmtUUID = (String) rel.getProperty(GraphStatics.UUID_PROPERTY, null);

		boolean stmtAlreadyProcessed = false;

		switch (deltaState) {

			case ADDITION:

				if (alreadyAddedStatementUUIDs.contains(newStmtUUID)) {

					stmtAlreadyProcessed = true;
				}

				break;
			case DELETION:

				if (alreadyDeletedStatementUUIDs.contains(newStmtUUID)) {

					stmtAlreadyProcessed = true;
				}

				break;
			case MODIFICATION:

				if (alreadyModifiedStatementUUIDs.contains(newStmtUUID)) {

					stmtAlreadyProcessed = true;
				}

				break;
		}

		return stmtAlreadyProcessed;
	}

	private Relationship getNewRel(final Iterator<Relationship> newRelationshipsIter, final Set<String> alreadyAddedStatementUUIDs,
			final Set<String> alreadyDeletedStatementUUIDs, final Set<String> alreadyModifiedNewStatementUUIDs) {

		final Relationship newRelationship = increaseRelationship(newRelationshipsIter);
		final DeltaState deltaState = getDeltaState(newRelationship);

		final boolean stmtAlreadyProcessed = checkStmt(newRelationship, deltaState, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs,
				alreadyModifiedNewStatementUUIDs);

		if (stmtAlreadyProcessed) {

			// skip new rel and fetch next one
			return getNewRel(newRelationshipsIter, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs, alreadyModifiedNewStatementUUIDs);
		}

		return newRelationship;
	}

	private Relationship increaseRelationship(final Iterator<Relationship> relationshipIterator) {

		if (relationshipIterator.hasNext()) {

			return relationshipIterator.next();
		}

		return null;
	}
}
