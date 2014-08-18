package org.dswarm.graph.gdm.parse;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.dswarm.graph.delta.Changeset;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.gdm.read.PropertyGraphGDMReader;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.ResourceNode;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class GDMChangesetParser implements GDMUpdateParser {

	private static final Logger				LOG						= LoggerFactory.getLogger(GDMChangesetParser.class);

	private GDMUpdateHandler				gdmHandler;
	private final Changeset					changeset;
	private final Resource					existingResource;
	private final GraphDatabaseService		existingResourceDB;
	private final GraphDatabaseService		newResourceDB;
	private final PropertyGraphGDMReader	propertyGraphGDMReader	= new PropertyGraphGDMReader();

	public GDMChangesetParser(final Changeset changesetArg, final Resource existingResourceArg, final GraphDatabaseService existingResourceDBArg,
			final GraphDatabaseService newResourceDBArg) {

		changeset = changesetArg;
		existingResource = existingResourceArg;
		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;
	}

	@Override
	public void setGDMHandler(final GDMUpdateHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public void parse() {

		if (changeset == null || existingResourceDB == null || newResourceDB == null) {

			LOG.debug("there is no change set or resource working sets");

			return;
		}

		// 0. fetch latest version from data model node (+ increase this value + update resource node (maybe at the end))
		final int latestVersion = gdmHandler.getLatestVersion();

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
			boolean							increaseNewRelationship	= true;
			Relationship newRelationship = null;

			do {

				final Long indexFromDB = (Long) existingRelationship.getProperty(GraphStatics.INDEX_PROPERTY, null);

				if (indexFromDB == null) {

					// TODO: we should probably should throw an exception instead

					break;
				}

				if(indexFromDB < index) {

					// TODO: should we do something here, e.g., shift every forthcoming statement further (because overall resource order has changed (?))

					System.out.println("there");
				}

				if (!newRelationshipsIter.hasNext()) {

					// TODO: we should probably should throw an exception instead

					break;
				}

				final DeltaState existingRelDeltaState = getDeltaState(existingRelationship);

				final DeltaState finalDeltaState;
				boolean increaseExistingRelationship = true;

				if(existingRelDeltaState.equals(DeltaState.ExactMatch)) {

					if(increaseNewRelationship) {

						newRelationship = getNewRel(newRelationshipsIter, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs,
								alreadyModifiedNewStatementUUIDs);

						increaseNewRelationship = true;
					}

					finalDeltaState = getDeltaState(newRelationship);
				} else {

					finalDeltaState = existingRelDeltaState;
					final boolean stmtAlreadyProcessed = checkStmt(existingRelationship, finalDeltaState, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs, alreadyModifiedExistingStatementUUIDs);

					if(stmtAlreadyProcessed) {

						// skip already processed existing stmt

						existingRelationship = existingRelationshipsIter.next();

						continue;
					}
				}

				switch (finalDeltaState) {

					case ADDITION:

						final String newResourceStmtUUID = (String) newRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
						final Statement addedStatement = changeset.getAdditions().get(newResourceStmtUUID);

						// retrieve start node via subject identifier (?) - start node must be a resource node (i.e., we could probably verify this requirement)
						// retrieve the resource identifier from the existing resource and replace the subject of the to-be-added statement
						if(ResourceNode.class.isInstance(addedStatement.getSubject())) {

							final String subjectURI = ((ResourceNode) addedStatement.getSubject()).getUri();

							if(!existingResource.getUri().equals(subjectURI)) {

								// TODO: do something, e.g., replace subject of the to-be-added statement;

								break;
							}
						}

						final String addedStmtUUID = addedStatement.getUUID();

						gdmHandler.handleStatement(addedStatement, existingResource, index);
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

						if(modifiedStatement != null) {

							finalModifiedStatement = modifiedStatement;
							existingModifiedStmtUUID = (String) existingRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
						} else {

							final Long newModifiedNodeId = newRelationship.getEndNode().getId();

							finalModifiedStatement = changeset.getNewModifiedStatements().get(newModifiedNodeId);

							Long existingModifiedNodeId = null;

							for(final Map.Entry<Long, Long> modificationsEntry : changeset.getModifications().entrySet()) {

								if(modificationsEntry.getValue().equals(newModifiedNodeId)) {

									existingModifiedNodeId = modificationsEntry.getKey();

									break;
								}
							}
							
							if(existingModifiedNodeId == null) {
								
								// TODO: do something, e.g., throw an exception
								
								break;
							}
							
							final Statement existingModifiedStatement = changeset.getExistingModifiedStatements().get(existingModifiedNodeId);
							existingModifiedStmtUUID = existingModifiedStatement.getUUID();

							increaseExistingRelationship = false;
						}

						if(finalModifiedStatement == null) {

							// TODO: do something, e.g., throw an exception

							break;
						}

						// utilise statement uuid from existing statenent to deprecate it + to get modification (?)
						final Node subject = gdmHandler.deprecateStatement(existingModifiedStmtUUID);
						alreadyModifiedExistingStatementUUIDs.add(existingModifiedStmtUUID);

						// take subject from existing resource to append the statement on the correct position
						finalModifiedStatement.setSubject(subject);

						gdmHandler.handleStatement(finalModifiedStatement, existingResource, index);
						alreadyModifiedNewStatementUUIDs.add(finalModifiedStatement.getUUID());

						index++;

						break;
				}

				if (!finalDeltaState.equals(DeltaState.ExactMatch)) {

					if(increaseExistingRelationship) {

						existingRelationship = existingRelationshipsIter.next();
					}

					continue;
				}

				if(newRelationship == null) {

					newRelationship = newRelationshipsIter.next();
				}

				final String existingRelationshipPrint = GraphDBUtil.printRelationship(existingRelationship);
				final String newRelationshipPrint = GraphDBUtil.printRelationship(newRelationship);

				if (existingRelationshipPrint.equals(newRelationshipPrint) && index == indexFromDB) {

					index++;

					existingRelationship = existingRelationshipsIter.next();

					continue;
				} else {

					// deprecate old statement and write it as new statement with a different index
					// TODO: propertyGraphGDMReader and GDMUpdateHandler need to share the same map for the bnodes + then we still need to ensure to do this mapping correctly
					// => we could also utilise the existing nodes of the bnodes of a statement, i.e., we need to read the existing stmt from the permanent DB instead of from the delta DB
					final String existingStmtUUID = (String) existingRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);

					gdmHandler.deprecateStatement(existingStmtUUID);

					gdmHandler.handleStatement(existingStmtUUID, existingResource, index);

					index++;

					increaseNewRelationship = false;
					existingRelationship = existingRelationshipsIter.next();

					// TODO: remove this, when continuing implementation
					break;
				}

			} while (newRelationshipsIter.hasNext());

			System.out.print(index);

			newResourceDBTX.success();
			existingResourceDBTX.success();
		} catch (final Exception e) {

			e.printStackTrace();

			newResourceDBTX.failure();
			existingResourceDBTX.failure();
		} finally {

			newResourceDBTX.close();
			existingResourceDBTX.close();
		}

		// 1.1 if a statement was added or deleted or the printed version doesn't equal, rewrite all following statements
		// afterwards and deprecated the existing ones (i.e. update their valid to value)
		// for added + modified statements utilise the current version for valid from

		// if (resource == null || resource.getStatements() == null || resource.getStatements().isEmpty()) {
		//
		// LOG.debug("there are no statements in the GDM resource");
		//
		// return;
		// }
		//
		// long i = 0;
		//
		// for (final Statement statement : resource.getStatements()) {
		//
		// i++;
		//
		// // note: just increasing the counter probably won't work at an update ;)
		//
		// gdmHandler.handleStatement(statement, resource, i);
		// }

		gdmHandler.closeTransaction();
		newResourceDB.shutdown();
		existingResourceDB.shutdown();
	}

	private DeltaState getDeltaState(final Relationship relationship) {

		final String deltaStateString = (String) relationship.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);

		return DeltaState.getByName(deltaStateString);
	}

	private boolean checkStmt(final Relationship rel, final DeltaState deltaState, final Set<String> alreadyAddedStatementUUIDs,
			final Set<String> alreadyDeletedStatementUUIDs, final Set<String> alreadyModifiedStatementUUIDs) {

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

		final Relationship newRelationship = newRelationshipsIter.next();
		final DeltaState deltaState = getDeltaState(newRelationship);

		final boolean stmtAlreadyProcessed = checkStmt(newRelationship, deltaState, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs,
				alreadyModifiedNewStatementUUIDs);

		if (stmtAlreadyProcessed) {

			// skip new rel and fetch next one
			return getNewRel(newRelationshipsIter, alreadyAddedStatementUUIDs, alreadyDeletedStatementUUIDs, alreadyModifiedNewStatementUUIDs);
		}

		return newRelationship;
	}
}
