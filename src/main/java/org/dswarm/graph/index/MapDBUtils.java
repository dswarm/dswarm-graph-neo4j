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
import java.util.Map;
import java.util.Set;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import org.dswarm.common.types.Tuple;

/**
 * @author tgaengler
 */
public final class MapDBUtils {

	public static final String INDEX_DIR = "index/mapdb";

	public static Tuple<Set<Long>, DB> createOrGetInMemoryLongIndexTreeSetNonTransactional(final String indexName) {

		final DB db = createNonTransactionalInMemoryMapDB();

		return Tuple.tuple(createTreeSet(db, indexName), db);
	}

	public static Tuple<Map<String, String>, DB> createOrGetInMemoryStringStringIndexTreeMapNonTransactional(final String indexName) {

		final DB db = createNonTransactionalInMemoryMapDB();

		return Tuple.tuple(createStringStringTreeMap(db, indexName), db);
	}

	public static Tuple<Set<Long>, DB> createOrGetPersistentLongIndexTreeSetGlobalTransactional(final String indexFileName, final String indexName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createTreeSet(db, indexName), db);
	}

	public static Tuple<Map<String, String>, DB> createOrGetPersistentStringStringIndexTreeMapGlobalTransactional(final String indexFileName, final String indexName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createStringStringTreeMap(db, indexName), db);
	}

	public static Map<String, String> createStringStringTreeMap(final DB db, final String indexFileName) {

		return db.createTreeMap(indexFileName)
				.keySerializer(BTreeKeySerializer.STRING)
				.valueSerializer(Serializer.STRING).makeOrGet();
	}

	public static Set<Long> createTreeSet(final DB db, final String indexFileName) {

		return db.createTreeSet(indexFileName).makeOrGet();
	}

	public static DB createNonTransactionalInMemoryMapDB() {

		return DBMaker.newMemoryDirectDB()
				.asyncWriteEnable()
				.closeOnJvmShutdown()
				.transactionDisable()
				.make();
	}

	public static DB createGlobalTransactionalPermanentMapDB(final String indexFileName) {

		final File file = createFile(indexFileName);

		return DBMaker
				.newFileDB(file)
				.asyncWriteEnable()
				.closeOnJvmShutdown()
				.make();
	}

	private static File createFile(final String indexFileName) {

		return new File(indexFileName + Statics.INDEX_FILE_ENDING);
	}
}
