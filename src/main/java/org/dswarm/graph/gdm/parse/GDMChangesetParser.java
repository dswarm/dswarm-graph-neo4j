package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.delta.Changeset;
import org.dswarm.graph.json.Statement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class GDMChangesetParser implements GDMParser {

	private static final Logger	LOG	= LoggerFactory.getLogger(GDMChangesetParser.class);

	private GDMHandler			gdmHandler;
	private final Changeset		changeset;
	private final GraphDatabaseService existingResourceDB;
	private final GraphDatabaseService newResourceDB;

	public GDMChangesetParser(final Changeset changesetArg, final GraphDatabaseService existingResourceDBArg, final GraphDatabaseService newResourceDBArg) {

		changeset = changesetArg;
		existingResourceDB = existingResourceDBArg;
		newResourceDB = newResourceDBArg;
	}

	@Override
	public void setGDMHandler(final GDMHandler handler) {

		gdmHandler = handler;
	}

	@Override
	public void parse() {

		if(changeset == null || existingResourceDB == null || newResourceDB == null) {

			LOG.debug("there is no change set or resource working sets");

			return;
		}

		// 0. fetch latest version from resource node (+ increase this value + update resource node (maybe at the end))
		// 1. compare existing resource DB statements with new resource DB statements, i.e. write/follow statements ordered by index property (it might be good to also have the relationship in the permanent DB that is related to this index available)
		// 1.1 if a statement was added or deleted or the printed version doesn't equal, rewrite all following statements afterwards and deprecated the existing ones (i.e. update their valid to value)
		// for added + modified statements utilise the current version for valid from

//		if (resource == null || resource.getStatements() == null || resource.getStatements().isEmpty()) {
//
//			LOG.debug("there are no statements in the GDM resource");
//
//			return;
//		}
//
//		long i = 0;
//
//		for (final Statement statement : resource.getStatements()) {
//
//			i++;
//
//			// note: just increasing the counter probably won't work at an update ;)
//
//			gdmHandler.handleStatement(statement, resource, i);
//		}

		gdmHandler.closeTransaction();
	}
}
