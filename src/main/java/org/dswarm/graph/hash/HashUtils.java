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
package org.dswarm.graph.hash;

import com.github.emboss.siphash.SipKey;

/**
 *
 */
public final class HashUtils {

	public static final SipKey	SPEC_KEY	= new SipKey(HashUtils.bytesOf(0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
													0x0c, 0x0d, 0x0e, 0x0f));

	private HashUtils() {
	}

	public static byte[] bytesOf(final Integer... bytes) {

		final byte[] ret = new byte[bytes.length];

		for (int i = 0; i < bytes.length; i++) {

			ret[i] = bytes[i].byteValue();
		}

		return ret;
	}

	public static byte[] byteTimes(final int b, final int times) {

		final byte[] ret = new byte[times];

		for (int i = 0; i < times; i++) {

			ret[i] = (byte) b;
		}

		return ret;
	}
}
