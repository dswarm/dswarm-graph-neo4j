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
import java.util.Set;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TxMaker;

import org.dswarm.common.types.Tuple;

/**
 * @author tgaengler
 */
public final class MapDBUtils {

	public static final String INDEX_DIR = "index/mapdb";
	public static final String DEFAULT_INDEX_NAME = "default_mapdb_index";

	public static Tuple<Map<Long, Long>, DB> createOrGetPersistentLongLongIndexTreeMapNonTransactional(final String indexFileName, final String indexName) {

		final DB db = createNonTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createTreeMap(db, indexName), db);
	}

	public static Tuple<Map<Long, Long>, DB> getOrCreateLongLongIndexTreeMapNonTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongLongIndexTreeMapNonTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name, name);
	}

	public static Tuple<Set<Long>, DB> createOrGetPersistentLongIndexTreeSetNonTransactional(final String indexFileName, final String indexName) {

		final DB db = createNonTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createTreeSet(db, indexName), db);
	}

	public static Tuple<Set<Long>, DB> getOrCreateLongIndexTreeSetNonTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongIndexTreeSetNonTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name, name);
	}

	public static Tuple<Set<Long>, DB> createOrGetInMemoryLongIndexTreeSetNonTransactional(final String indexName) {

		final DB db = createNonTransactionalInMemoryMapDB();

		return Tuple.tuple(createTreeSet(db, indexName), db);
	}

	public static Tuple<Map<String, String>, DB> createOrGetInMemoryStringStringIndexTreeMapNonTransactional(final String indexName) {

		final DB db = createNonTransactionalInMemoryMapDB();

		return Tuple.tuple(createStringStringTreeMap(db, indexName), db);
	}

	public static Tuple<Map<String, String>, DB> createOrGetInMemoryStringStringIndexTreeMapGlobalTransactional(final String indexName) {

		final DB db = createGlobalTransactionalInMemoryMapDB();

		return Tuple.tuple(createStringStringTreeMap(db, indexName), db);
	}

	public static Tuple<Set<Long>, DB> getOrCreateInMemoryLongIndexTreeSetNonTransactional(final String name) {

		return MapDBUtils.createOrGetInMemoryLongIndexTreeSetNonTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static Tuple<Map<Long, Long>, DB> createOrGetPersistentLongLongIndexTreeMapGlobalTransactional(final String indexFileName, final String indexName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createTreeMap(db, indexName), db);
	}

	public static Tuple<Map<Long, Long>, DB> getOrCreateLongLongIndexTreeMapGlobalTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongLongIndexTreeMapGlobalTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name, name);
	}

	public static Tuple<Set<Long>, DB> createOrGetPersistentLongIndexTreeSetGlobalTransactional(final String indexFileName, final String indexName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createTreeSet(db, indexName), db);
	}

	public static Tuple<Set<Long>, DB> getOrCreateLongIndexTreeSetGlobalTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongIndexTreeSetGlobalTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name, name);
	}

	public static Tuple<Set<Long>, DB> createOrGetInMemoryLongIndexTreeSetGlobalTransactional(final String indexFileName) {

		final DB db = createGlobalTransactionalInMemoryMapDB();

		return Tuple.tuple(createTreeSet(db, indexFileName), db);
	}

	public static Tuple<Set<Long>, DB> getOrCreateInMemoryLongIndexTreeSetGlobalTransactional(final String name) {

		return MapDBUtils.createOrGetInMemoryLongIndexTreeSetGlobalTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static TxMaker createOrGetPersistentIndexTransactional(final String indexFileName) throws IOException {

		final File file = new File(indexFileName + Statics.INDEX_FILE_ENDING);

		return DBMaker.newFileDB(file).asyncWriteEnable().closeOnJvmShutdown().makeTxMaker();
	}

	public static TxMaker getOrCreateIndexTransactional(final String name) throws IOException {

		return MapDBUtils.createOrGetPersistentIndexTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static Tuple<Map<Long, Long>, DB> createOrGetPersistentLongLongIndexHashMapNonTransactional(final String indexFileName) {

		final DB db = createNonTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createHashMap(db, indexFileName), db);
	}

	public static Tuple<Map<Long, Long>, DB> getOrCreateLongLongIndexHashMapNonTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongLongIndexHashMapNonTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static Tuple<Set<Long>, DB> createOrGetPersistentLongIndexHashSetNonTransactional(final String indexFileName) {

		final DB db = createNonTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createHashSet(db, indexFileName), db);
	}

	public static Tuple<Set<Long>, DB> getOrCreateLongIndexHashSetNonTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongIndexHashSetNonTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static Tuple<Map<Long, Long>, DB> createOrGetPersistentLongLongIndexHashMapGlobalTransactional(final String indexFileName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createHashMap(db, indexFileName), db);
	}

	public static Tuple<Map<Long, Long>, DB> getOrCreateLongLongIndexHashMapGlobalTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongLongIndexHashMapGlobalTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static Tuple<Map<String, String>, DB> createOrGetPersistentStringStringIndexTreeMapGlobalTransactional(final String indexFileName, final String indexName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createStringStringTreeMap(db, indexName), db);
	}

	public static Tuple<Map<String, String>, DB> getOrCreateStringStringndexTreeMapGlobalTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentStringStringIndexTreeMapGlobalTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name, name);
	}

	public static Tuple<Set<Long>, DB> createOrGetPersistentLongIndexHashSetGlobalTransactional(final String indexFileName) {

		final DB db = createGlobalTransactionalPermanentMapDB(indexFileName);

		return Tuple.tuple(createHashSet(db, indexFileName), db);
	}

	public static Tuple<Set<Long>, DB> getOrCreateLongIndexHashSetGlobalTransactional(final String name) {

		return MapDBUtils.createOrGetPersistentLongIndexHashSetGlobalTransactional(Statics.DEFAULT_STORE_DIR + File.separator + name);
	}

	public static Map<Long, Long> createTreeMap(final DB db, final String indexFileName) {

		return db.createTreeMap(indexFileName)
				//.keySerializer(BTreeKeySerializer.LONG) // supported by 2.0-alpha1
				.valueSerializer(Serializer.LONG).makeOrGet();
	}

	public static Map<String, String> createStringStringTreeMap(final DB db, final String indexFileName) {

		return db.createTreeMap(indexFileName)
				.keySerializer(BTreeKeySerializer.STRING)
				.valueSerializer(Serializer.STRING).makeOrGet();
	}

	public static Set<Long> createTreeSet(final DB db, final String indexFileName) {

		return db.createTreeSet(indexFileName).makeOrGet();
	}

	public static Map<Long, Long> createHashMap(final DB db, final String indexFileName) {

		return db.createHashMap(indexFileName)
				//.keySerializer(Serializer.LONG) // supported by 2.0-alpha1
				.valueSerializer(Serializer.LONG).makeOrGet();
	}

	public static Set<Long> createHashSet(final DB db, final String indexFileName) {

		return db.createHashSet(indexFileName).serializer(Serializer.LONG).makeOrGet();
	}

	public static DB createNonTransactionalPermanentMapDB(final String indexFileName) {

		final File file = createFile(indexFileName);

		return DBMaker
				.newFileDB(file)
				.asyncWriteEnable()
						// TODO: enable if, this performs better over time
						//.compressionEnable().asyncWriteFlushDelay(1)
				.closeOnJvmShutdown()
				.transactionDisable()
				.make();
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

	public static DB createGlobalTransactionalInMemoryMapDB() {

		return DBMaker.newMemoryDirectDB()
				.asyncWriteEnable()
				.closeOnJvmShutdown()
				.make();
	}

	private static File createFile(final String indexFileName) {

		return new File(indexFileName + Statics.INDEX_FILE_ENDING);
	}
}
