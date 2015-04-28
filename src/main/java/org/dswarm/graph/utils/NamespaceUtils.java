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
package org.dswarm.graph.utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public class NamespaceUtils {

	private static final AtomicInteger counter               = new AtomicInteger(0);
	private static final String        NAMESPACE_PREFIX_BASE = "ns";
	public static final char PREFIX_DELIMITER = ':';

	public static String getPrefix(final String namespace, final Map<String, String> tempNamespacesPrefixesMap,
			final Map<String, String> inMemoryNamespacesPrefixesMap, final Map<String, String> permanentNamespacesPrefixesMap)
			throws DMPGraphException {

		if (namespace == null || namespace.trim().isEmpty()) {

			throw new DMPGraphException("namespace shouldn't be null or empty");
		}

		if (tempNamespacesPrefixesMap.containsKey(namespace)) {

			return tempNamespacesPrefixesMap.get(namespace);
		}

		if (inMemoryNamespacesPrefixesMap.containsKey(namespace)) {

			return inMemoryNamespacesPrefixesMap.get(namespace);
		}

		if (permanentNamespacesPrefixesMap.containsKey(namespace)) {

			final String prefix = permanentNamespacesPrefixesMap.get(namespace);

			// cache in-memory
			inMemoryNamespacesPrefixesMap.put(namespace, prefix);

			return prefix;
		}

		final String prefix = NAMESPACE_PREFIX_BASE + counter.incrementAndGet();
		tempNamespacesPrefixesMap.put(namespace, prefix);

		return prefix;
	}
}
