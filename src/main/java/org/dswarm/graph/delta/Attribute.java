package org.dswarm.graph.delta;

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
