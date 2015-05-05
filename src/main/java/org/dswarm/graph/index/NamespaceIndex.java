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
package org.dswarm.graph.index;

import java.util.Map;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.vocabulary.RDFS;
import org.mapdb.DB;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.types.Tuple;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.GraphProcessingStatics;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.tx.TransactionHandler;
import org.dswarm.graph.utils.NamespaceUtils;

/**
 * @author tgaengler
 */
public class NamespaceIndex {

	private static final Logger LOG = LoggerFactory.getLogger(NamespaceIndex.class);

	// for caching per TX
	final private Map<String, String> tempNamespacePrefixes;
	final private DB                  tempNamespacePrefixesDB;

	// for caching over the whole process
	final private Map<String, String> inMemoryNamespacePrefixes;
	final private DB                  inMemoryNamespacePrefixesDB;

	private final Map<String, String> uriPrefixedURIMap;
	private final Map<String, String> prefixedURIURIMap;

	private final GraphDatabaseService database;
	private final TransactionHandler tx;

	public NamespaceIndex(final GraphDatabaseService databaseArg, final TransactionHandler txArg) {

		database = databaseArg;
		tx = txArg;

		uriPrefixedURIMap = Maps.newHashMap();
		prefixedURIURIMap = Maps.newHashMap();

		final Tuple<Map<String, String>, DB> mapDBTuple3 = MapDBUtils.createOrGetInMemoryStringStringIndexTreeMapNonTransactional(
				GraphIndexStatics.TEMP_NAMESPACE_PREFIXES_INDEX_NAME);
		tempNamespacePrefixes = mapDBTuple3.v1();
		tempNamespacePrefixesDB = mapDBTuple3.v2();

		final Tuple<Map<String, String>, DB> mapDBTuple4 = MapDBUtils.createOrGetInMemoryStringStringIndexTreeMapNonTransactional(
				GraphIndexStatics.IN_MEMORY_NAMESPACE_PREFIXES_INDEX_NAME);
		inMemoryNamespacePrefixes = mapDBTuple4.v1();
		inMemoryNamespacePrefixesDB = mapDBTuple4.v2();
	}

	public void resetTXNamespaces() {

		if (tempNamespacePrefixes != null) {

			tempNamespacePrefixes.clear();
		}
	}

	public String createPrefixedURI(final String fullURI) throws DMPGraphException {

		return NamespaceUtils.createPrefixedURI(fullURI, uriPrefixedURIMap, tempNamespacePrefixes, inMemoryNamespacePrefixes, database, tx);
	}

	public String createFullURI(final String prefixedURI) throws DMPGraphException {

		return NamespaceUtils.createFullURI(prefixedURI, prefixedURIURIMap, database, tx);
	}

	public String getRDFCLASSPrefixedURI() throws DMPGraphException {

		return createPrefixedURI(RDFS.Class.getURI());
	}

	public void pumpNFlushNamespacePrefixIndex() throws DMPGraphException {

		LOG.debug("start pump'n'flushing namespace prefix index; size = '{}'", tempNamespacePrefixes.size());

		for (final Map.Entry<String, String> entry : tempNamespacePrefixes.entrySet()) {

			final String namespace = entry.getKey();
			final String prefix = entry.getValue();

			inMemoryNamespacePrefixes.put(namespace, prefix);

			try {

				tx.ensureRunningTx();

				final Node prefixNode = database.createNode(GraphProcessingStatics.PREFIX_LABEL);
				prefixNode.setProperty(GraphStatics.URI_PROPERTY, namespace);
				prefixNode.setProperty(GraphProcessingStatics.PREFIX_PROPERTY, prefix);
			} catch (final Exception e) {

				tx.failTx();

				final String msg = "couldn't pump'n'flush namespace prefix index successfully";

				LOG.error(msg);

				throw new DMPGraphException(msg, e);
			}
		}

		LOG.debug("finished pumping namespace prefix index");

		tempNamespacePrefixesDB.commit();
		inMemoryNamespacePrefixesDB.commit();

		LOG.debug("finished flushing namespace prefix index");
	}

	public void clearMaps() {

		uriPrefixedURIMap.clear();
		prefixedURIURIMap.clear();

		if (!tempNamespacePrefixesDB.isClosed()) {

			tempNamespacePrefixes.clear();
			tempNamespacePrefixesDB.close();
		}

		if (!inMemoryNamespacePrefixesDB.isClosed()) {

			inMemoryNamespacePrefixes.clear();
			inMemoryNamespacePrefixesDB.close();
		}
	}

	public void closeMapDBIndices() {

		closeMapDBIndex(tempNamespacePrefixesDB);
		closeMapDBIndex(inMemoryNamespacePrefixesDB);
	}

	private void closeMapDBIndex(final DB mapDBIndex) {

		if (mapDBIndex != null && !mapDBIndex.isClosed()) {

			mapDBIndex.close();
		}
	}

}
