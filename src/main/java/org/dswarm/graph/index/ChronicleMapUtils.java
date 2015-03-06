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
/**
 * This file is part of d:swarm graph extension. d:swarm graph extension is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version. d:swarm graph extension is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with d:swarm
 * graph extension. If not, see <http://www.gnu.org/licenses/>.
 */
package org.dswarm.graph.index;

import java.io.File;
import java.io.IOException;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * @author tgaengler
 */
public final class ChronicleMapUtils {

	private static final String	INDEX_FILE_ENDING	= ".dat";

	public static final String	INDEX_DIR			= "cmindex";

	public static ChronicleMap<Long, Long> createOrGetPersistentLongIndex(final String indexFileName) throws IOException {

		final File file = new File(indexFileName + INDEX_FILE_ENDING);

		final ChronicleMapBuilder<Long, Long> builder = getLongLongChronicleMapBuilder();

		return builder.entries(Integer.MAX_VALUE).createPersistedTo(file);
	}

	public static ChronicleMap<Long, Long> createOrGetLongIndex() {

		final ChronicleMapBuilder<Long, Long> builder = getLongLongChronicleMapBuilder();

		return builder.minSegments(10000).entriesPerSegment(10 * 1000).create();
	}

	private static ChronicleMapBuilder<Long, Long> getLongLongChronicleMapBuilder() {

		// TODO: optimize builder, e.g., set chunk size
		return ChronicleMapBuilder.of(Long.class, Long.class).constantKeySizeBySample(Long.MAX_VALUE).constantValueSizeBySample(Long.MAX_VALUE);
	}
}
