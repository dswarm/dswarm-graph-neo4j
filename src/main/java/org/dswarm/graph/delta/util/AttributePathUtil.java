package org.dswarm.graph.delta.util;

import java.util.LinkedList;

import org.dswarm.graph.delta.Attribute;
import org.dswarm.graph.delta.DMPStatics;

/**
 * Created by tgaengler on 29/07/14.
 */
public final class AttributePathUtil {

	public static final String generateAttributePath(final LinkedList<Attribute> attributes) {

		if (attributes == null || attributes.isEmpty()) {

			return null;
		}

		final StringBuilder sb = new StringBuilder();

		for (int i = 0; i < attributes.size(); i++) {

			sb.append(attributes.get(i));

			if (i < (attributes.size() - 1)) {

				sb.append(DMPStatics.ATTRIBUTE_DELIMITER);
			}
		}

		return sb.toString();
	}
}
