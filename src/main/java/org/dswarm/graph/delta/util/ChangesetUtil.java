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
