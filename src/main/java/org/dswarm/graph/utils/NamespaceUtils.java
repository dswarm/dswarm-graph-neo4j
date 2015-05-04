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

import com.google.common.base.Optional;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import org.dswarm.common.types.Tuple;
import org.dswarm.common.web.URI;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.model.GraphStatics;

/**
 * @author tgaengler
 */
public class NamespaceUtils {

	private static final AtomicInteger counter               = new AtomicInteger(0);
	private static final String        NAMESPACE_PREFIX_BASE = "ns";
	public static final  char          PREFIX_DELIMITER      = ':';

	public static String createPrefixedURI(final String fullURI, final Map<String, String> uriPrefixedURIMap,
			final Map<String, String> tempNamespacePrefixes, final Map<String, String> inMemoryNamespacePrefixes, final GraphDatabaseService database)
			throws DMPGraphException {

		if (fullURI == null) {

			throw new DMPGraphException("full URI shouldn't be null");
		}

		if (uriPrefixedURIMap != null) {

			if (!uriPrefixedURIMap.containsKey(fullURI)) {

				final String prefixedURI = determinePrefixedURI(fullURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, database);

				uriPrefixedURIMap.put(fullURI, prefixedURI);
			}

			return uriPrefixedURIMap.get(fullURI);
		} else {

			return determinePrefixedURI(fullURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, database);
		}
	}

	public static String getPrefix(final String namespace, final Map<String, String> tempNamespacesPrefixesMap,
			final Map<String, String> inMemoryNamespacesPrefixesMap, final GraphDatabaseService database)
			throws DMPGraphException {

		if (namespace == null || namespace.trim().isEmpty()) {

			throw new DMPGraphException("namespace shouldn't be null or empty");
		}

		if (tempNamespacesPrefixesMap != null && tempNamespacesPrefixesMap.containsKey(namespace)) {

			return tempNamespacesPrefixesMap.get(namespace);
		}

		if (inMemoryNamespacesPrefixesMap != null && inMemoryNamespacesPrefixesMap.containsKey(namespace)) {

			return inMemoryNamespacesPrefixesMap.get(namespace);
		}

		final Optional<Node> optionalNode = Optional
				.fromNullable(database.findNode(GraphProcessingStatics.PREFIX_LABEL, GraphStatics.URI_PROPERTY, namespace));

		if (optionalNode.isPresent()) {

			final String prefix = (String) optionalNode.get().getProperty(GraphProcessingStatics.PREFIX_PROPERTY);

			if (inMemoryNamespacesPrefixesMap != null) {

				// cache in-memory
				inMemoryNamespacesPrefixesMap.put(namespace, prefix);
			}

			return prefix;
		}

		final String prefix = NAMESPACE_PREFIX_BASE + counter.incrementAndGet();

		if (tempNamespacesPrefixesMap != null) {

			tempNamespacesPrefixesMap.put(namespace, prefix);
		}

		return prefix;
	}

	public static String determinePrefixedURI(final String fullURI, final Map<String, String> tempNamespacePrefixes,
			final Map<String, String> inMemoryNamespacePrefixes, final GraphDatabaseService database) throws DMPGraphException {

		final Tuple<String, String> uriParts = URI.determineParts(fullURI);
		final String namespaceURI = uriParts.v1();
		final String localName = uriParts.v2();

		final String prefix = NamespaceUtils.getPrefix(namespaceURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, database);

		return prefix + NamespaceUtils.PREFIX_DELIMITER + localName;
	}
}
