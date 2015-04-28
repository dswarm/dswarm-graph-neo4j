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
package org.dswarm.graph;

/**
 * The node type enum. A node type indicates the type of a node.<br>
 *
 * @author tgaengler (created), Mar 18, 2013
 * @author $Author$ (last changed)
 * @version $Rev$, $Date$<br>
 *          $Id: $
 */

public enum NodeType {

	/**
	 * Type for resources.
	 */
	Resource("RESOURCE"),

	/**
	 * Type for bnodes.
	 */
	BNode("BNODE"),

	/**
	 * Type for resources that are types/classes.
	 */
	TypeResource("TYPE_RESOURCE"),

	/**
	 * Type for bnodes that are types/classes.
	 */
	TypeBNode("TYPE_BNODE"),

	/**
	 * Type for literals.
	 */
	Literal("LITERAL");

	/**
	 * The name of the node type.
	 */
	private final String	name;

	/**
	 * Gets the name of the node type.
	 *
	 * @return the name of the node type
	 */
	public String getName() {

		return name;
	}

	/**
	 * Creates a new node type with the given name.
	 *
	 * @param nameArg the name of the node type.
	 */
	private NodeType(final String nameArg) {

		this.name = nameArg;
	}

	/**
	 * Gets the node type by the given name, e.g. 'FUNCTION' or 'TRANSFORMATION'.<br>
	 * Created by: ydeng
	 *
	 * @param name the name of the node type
	 * @return the appropriated node type
	 */
	public static NodeType getByName(final String name) {

		for (final NodeType functionType : NodeType.values()) {

			if (functionType.name.equals(name)) {

				return functionType;
			}
		}

		throw new IllegalArgumentException(name);
	}

	/**
	 * {@inheritDoc}<br>
	 * Returns the name of the node type.
	 *
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString() {

		return name;
	}
}
