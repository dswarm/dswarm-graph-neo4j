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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import javax.management.ObjectName;

import com.google.common.io.Resources;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.jmx.JmxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.index.MapDBUtils;

/**
 * @author tgaengler
 */
public final class GraphDatabaseUtils {

	private static final Logger LOG = LoggerFactory.getLogger(GraphDatabaseUtils.class);
	private static final String CONFIGURATION = "Configuration";
	private static final String STORE_DIR     = "store_dir";

	public static String determineMapDBIndexStoreDir(final GraphDatabaseService database) {

		final URL resource = Resources.getResource("dmpgraph.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load dmpgraph.properties", e);
		}

		final String indexStoreDir = properties.getProperty("index_store_dir");

		// TODO: find a better way to determine the store dir for the statement index
		String storeDir = null;

		if (indexStoreDir != null && !indexStoreDir.trim().isEmpty()) {

			storeDir = indexStoreDir;
		} else if (getGraphDatabaseStoreDir(database) != null) {

			final String databaseStoreDir = getGraphDatabaseStoreDir(database);

			if (databaseStoreDir != null) {

				final Path databaseStoreDirPath = Paths.get(databaseStoreDir);

				if (databaseStoreDirPath != null) {

					if (Files.exists(databaseStoreDirPath)) {

						final String newIndexStoreDir = databaseStoreDir + File.separator + MapDBUtils.INDEX_DIR;

						final Path newIndexStoreDirPath = Paths.get(newIndexStoreDir);

						if (Files.notExists(newIndexStoreDirPath)) {

							try {

								Files.createDirectory(newIndexStoreDirPath);

								storeDir = newIndexStoreDir;
							} catch (final IOException e) {

								LOG.debug("couldn't create map db index directory");
							}
						} else {

							storeDir = newIndexStoreDir;
						}
					}
				}
			}
		}

		if (storeDir == null) {

			// fallback default
			storeDir = System.getProperty("user.dir") + "/target";
		}

		LOG.debug("mapdb index store dir '{}'", storeDir);

		return storeDir;
	}

	public static String getGraphDatabaseStoreDir(final GraphDatabaseService graphDbService) {

		final ObjectName objectName = JmxUtils.getObjectName(graphDbService, CONFIGURATION);
		return JmxUtils.getAttribute(objectName, STORE_DIR);
	}
}
