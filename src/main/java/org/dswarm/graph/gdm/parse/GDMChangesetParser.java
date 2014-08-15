package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.delta.Changeset;
import org.dswarm.graph.delta.DeltaState;
import org.dswarm.graph.delta.DeltaStatics;
import org.dswarm.graph.delta.util.GraphDBUtil;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.VersioningStatics;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author tgaengler
 */
public class GDMChangesetParser implements GDMUpdateParser {

	private static final Logger			LOG	= LoggerFactory.getLogger(GDMChangesetParser.class);

	private GDMUpdateHandler			gdmHandler;
	private final Changeset				changeset;
	private final Resource				existingResource;
	private final GraphDatabaseService	existingResourceDB;
	private final GraphDatabaseService	newResourceDB;

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
			final Iterable<Relationship> newRelationships = GlobalGraphOperations.at(newResourceDB).getAllRelationships();
			final Iterator<Relationship> newRelationshipsIter = newRelationships.iterator();

			long index = 0;

			for (final Relationship existingRelationship : existingRelationships) {

				final Long indexFromDB = (Long) existingRelationship.getProperty(GraphStatics.INDEX_PROPERTY, null);

				if(indexFromDB == null) {

					// TODO: we should propably should throw an exception instead

					break;
				}

				index = indexFromDB;

				if(!newRelationshipsIter.hasNext()) {

					// TODO: we should propably should throw an exception instead

					break;
				}

				final String deltaStateString = (String) existingRelationship.getProperty(DeltaStatics.DELTA_STATE_PROPERTY, null);
				final DeltaState deltaState = DeltaState.getByName(deltaStateString);

				switch(deltaState) {

					case ADDITION:

						break;
					case DELETION:

						break;
					case MODIFICATION:

						// utilise statement uuid from existing statenent to deprecate it + to get modification (?)
						final String uuid = (String) existingRelationship.getProperty(GraphStatics.UUID_PROPERTY, null);
						final Node subject = gdmHandler.deprecateStatement(uuid);

						final Long modifiedNodeId = changeset.getModifications().get(existingRelationship.getEndNode().getId());
						final Collection<Statement> modifiedStatements = changeset.getNewModifiedStatements().get(modifiedNodeId);
						final Statement modifiedStatement = modifiedStatements.iterator().next();

						modifiedStatement.setSubject(subject);

						gdmHandler.handleStatement(modifiedStatement, existingResource, index);

						break;
				}

				if(!deltaState.equals(DeltaState.ExactMatch)) {

					break;
				}

				final Relationship newRelationship = newRelationshipsIter.next();

				final String existingRelationshipPrint = GraphDBUtil.printRelationship(existingRelationship);
				final String newRelationshipPrint = GraphDBUtil.printRelationship(newRelationship);

				if(existingRelationship.equals(newRelationship)) {

					continue;
				} else {

					System.out.println("HERE");
				}


			}

			System.out.print(index);

			newResourceDBTX.success();
			existingResourceDBTX.success();
		} catch (final Exception e) {

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
}
