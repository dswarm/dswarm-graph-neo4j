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

/**
 * @author tgaengler
 */
public final class Statics {

	public static final String INDEX_FILE_ENDING = ".dat";

	public static final String USER_DIR_IDENTIFIER = "user.dir";

	public static final String USER_DIR = System.getProperty(USER_DIR_IDENTIFIER);

	public static final String TARGET_IDENTIFIER = "target";

	public static final String DEFAULT_STORE_DIR = USER_DIR + File.separator + TARGET_IDENTIFIER;
}
