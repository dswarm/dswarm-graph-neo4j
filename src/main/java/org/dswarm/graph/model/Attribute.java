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

/**
 * Created by tgaengler on 29/07/14.
 */
public class Attribute {

	private final String	uri;

	public Attribute(final String uriArg) {

		uri = uriArg;
	}

	public String getUri() {

		return uri;
	}

	@Override
	public boolean equals(final Object o) {

		if (this == o) {

			return true;
		}
		if (o == null || getClass() != o.getClass()) {

			return false;
		}

		final Attribute attribute = (Attribute) o;

		if (!uri.equals(attribute.uri)) {

			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {

		return uri.hashCode();
	}

	@Override
	public String toString() {

		return uri;
	}
}
