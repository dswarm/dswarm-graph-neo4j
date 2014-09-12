package org.dswarm.graph.delta.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public final class ChangesetUtil {

	public static Map<Long, Statement> providedModifiedStatements(final Map<String, Statement> modifiedStatements) {

		final Map<Long, Statement> newModifiedStatements = new LinkedHashMap<>();

		for(final Map.Entry<String, Statement> modifiedStatementsEntry : modifiedStatements.entrySet()) {

			newModifiedStatements.put(Long.valueOf(modifiedStatementsEntry.getKey()), modifiedStatementsEntry.getValue());
		}

		return newModifiedStatements;
	}
}
