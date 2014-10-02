package org.dswarm.graph.hash;

/**
 *
 */
public final class HashUtils {

	private HashUtils() {}

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

