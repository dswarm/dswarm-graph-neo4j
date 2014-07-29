package org.dswarm.graph.delta;

import org.dswarm.graph.delta.util.AttributePathUtil;

import java.util.LinkedList;

/**
 * Created by tgaengler on 29/07/14.
 */
public class AttributePath {

	private LinkedList<Attribute> attributes;

	public AttributePath(LinkedList<Attribute> attributesArg) {

		attributes = attributesArg;
	}

	public LinkedList<Attribute> getAttributes() {

		return attributes;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		AttributePath that = (AttributePath) o;

		if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) {

			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {

		return attributes != null ? attributes.hashCode() : 0;
	}

	@Override public String toString() {

		return AttributePathUtil.generateAttributePath(attributes);
	}
}
