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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.types.Tuple;
import org.dswarm.common.web.URI;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;

/**
 * @author tgaengler
 */
public class NamespaceUtils {

	private static final Logger LOG = LoggerFactory.getLogger(NamespaceUtils.class);

	private static final AtomicInteger counter                = new AtomicInteger(0);
	private static final String        NAMESPACE_PREFIX_BASE  = "ns";
	public static final  char          PREFIX_DELIMITER       = ':';
	public static final  String        PREFIX_DELIMTER_STRING = String.valueOf(PREFIX_DELIMITER);

	public static String createPrefixedURI(final String fullURI, final Map<String, String> uriPrefixedURIMap,
			final Map<String, String> tempNamespacePrefixes, final Map<String, String> inMemoryNamespacePrefixes, final GraphDatabaseService database,
			final TransactionHandler tx)
			throws DMPGraphException {

		if (fullURI == null) {

			throw new DMPGraphException("full URI shouldn't be null");
		}

		if (uriPrefixedURIMap != null) {

			if (!uriPrefixedURIMap.containsKey(fullURI)) {

				final String prefixedURI = determinePrefixedURI(fullURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, database, tx);

				uriPrefixedURIMap.put(fullURI, prefixedURI);
			}

			return uriPrefixedURIMap.get(fullURI);
		} else {

			return determinePrefixedURI(fullURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, database, tx);
		}
	}

	public static String createFullURI(final String prefixedURI, final Map<String, String> prefixedURIURIMap, final GraphDatabaseService database,
			final TransactionHandler tx) throws DMPGraphException {

		if (prefixedURI == null) {

			throw new DMPGraphException("prefixed URI shouldn't be null");
		}

		if (prefixedURIURIMap != null) {

			if (!prefixedURIURIMap.containsKey(prefixedURI)) {

				final String fullURI = determineFullURI(prefixedURI, database, tx);

				prefixedURIURIMap.put(prefixedURI, fullURI);
			}

			return prefixedURIURIMap.get(prefixedURI);
		} else {

			return determineFullURI(prefixedURI, database, tx);
		}
	}

	public static String getPrefix(final String namespace, final Map<String, String> tempNamespacesPrefixesMap,
			final Map<String, String> inMemoryNamespacesPrefixesMap, final GraphDatabaseService database, final TransactionHandler tx)
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

		try {

			tx.ensureRunningTx();

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
		} catch (final Exception e) {

			tx.failTx();

			final String message = "couldn't retrieve prefix successfully";

			LOG.error(message);

			throw new DMPGraphException(message, e);
		}
	}

	public static String getNamespace(final String prefix, final GraphDatabaseService database, final TransactionHandler tx) throws DMPGraphException {

		if (prefix == null || prefix.trim().isEmpty()) {

			throw new DMPGraphException("prefix shouldn't be null or empty");
		}

		Optional<Node> optionalNode = null;

		try {

			tx.ensureRunningTx();

			optionalNode = Optional.fromNullable(
					database.findNode(GraphProcessingStatics.PREFIX_LABEL, GraphProcessingStatics.PREFIX_PROPERTY, prefix));
		} catch (final Exception e) {

			tx.failTx();

			final String message = "couldn't retrieve namespace successfully";

			LOG.error(message);

			throw new DMPGraphException(message, e);
		}

		if (optionalNode == null || !optionalNode.isPresent()) {

			throw new DMPGraphException(String.format("couldn't find a namespace for prefix '%s'", prefix));
		}

		final Node node = optionalNode.get();
		return (String) node.getProperty(GraphStatics.URI_PROPERTY);
	}

	public static String determinePrefixedURI(final String fullURI, final Map<String, String> tempNamespacePrefixes,
			final Map<String, String> inMemoryNamespacePrefixes, final GraphDatabaseService database, final TransactionHandler tx) throws DMPGraphException {

		final Tuple<String, String> uriParts = URI.determineParts(fullURI);
		final String namespaceURI = uriParts.v1();
		final String localName = uriParts.v2();

		final String prefix = NamespaceUtils.getPrefix(namespaceURI, tempNamespacePrefixes, inMemoryNamespacePrefixes, database, tx);

		return prefix + NamespaceUtils.PREFIX_DELIMITER + localName;
	}

	public static String determineFullURI(final String prefixedURI, final GraphDatabaseService database, final TransactionHandler tx) throws DMPGraphException {

		final String[] splittedPrefixedURI = prefixedURI.split(PREFIX_DELIMTER_STRING);

		if (splittedPrefixedURI.length != 2) {

			throw new DMPGraphException(String.format("couldn't split prefixed URI '%s' into two parts", prefixedURI));
		}

		final String prefix = splittedPrefixedURI[0];
		final String localName = splittedPrefixedURI[1];

		final String namespace = getNamespace(prefix, database, tx);

		return namespace + localName;
	}
}
