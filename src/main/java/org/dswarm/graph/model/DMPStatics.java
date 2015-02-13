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
package org.dswarm.graph.model;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

/**
 * Holds references for static fields.
 * 
 * @author tgaengler
 */
public interface DMPStatics {

	/**
	 * The delimiter of an attribute path.
	 */
	public static final Character	ATTRIBUTE_DELIMITER	= '\u001E';

	public static final String RECORD_CLASS_URI_IDENTIFIER = "record_class_uri";

	public static final String DATA_MODEL_URI_IDENTIFIER = "data_model_uri";

	public static final String VERSION_IDENTIFIER = "version";

	public static final String ROOT_ATTRIBUTE_PATH_IDENTIFIER = "root_attribute_path";
}
