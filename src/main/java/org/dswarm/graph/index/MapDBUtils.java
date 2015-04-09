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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import org.dswarm.common.types.Tuple;

/**
 * @author tgaengler
 */
public final class MapDBUtils {

	private static final String INDEX_FILE_ENDING = ".dat";

	public static final String INDEX_DIR = "cmindex";

	public static final String DEFAULT_INDEX_NAME = "default_mapdb_index";

	public static Tuple<Map<Long, Long>, DB> createOrGetPersistentLongIndex(final String indexFileName) throws IOException {

		final File file = new File(indexFileName + INDEX_FILE_ENDING);

		final DB db = DBMaker
				.newFileDB(file).asyncWriteEnable()
						/** disabling Write Ahead Log makes import much faster */
				//.transactionDisable()
				.make();

		final Map<Long, Long> map = db.createTreeMap(indexFileName).keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
				.valueSerializer(Serializer.LONG).makeOrGet();

		return Tuple.tuple(map, db);
	}

	public static Tuple<Map<Long, Long>, DB> getOrCreateLongIndex(final String name) throws IOException {

		final String storeDir = System.getProperty("user.dir");

		// + File.separator + ChronicleMapUtils.INDEX_DIR
		return MapDBUtils.createOrGetPersistentLongIndex(storeDir + File.separator + name);
	}
}
