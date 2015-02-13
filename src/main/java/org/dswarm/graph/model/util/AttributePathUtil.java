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
package org.dswarm.graph.model.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.dswarm.graph.model.Attribute;
import org.dswarm.graph.model.AttributePath;
import org.dswarm.graph.model.ContentSchema;
import org.dswarm.graph.model.DMPStatics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Optional;

/**
 * Created by tgaengler on 29/07/14.
 */
public final class AttributePathUtil {

	public static String generateAttributePath(final LinkedList<Attribute> attributes) {

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

	public static Optional<AttributePath> determineCommonAttributePath(final ContentSchema contentSchema) {

		if(contentSchema.getKeyAttributePaths() == null && contentSchema.getValueAttributePath() == null) {

			return Optional.absent();
		}

		final Map<String, AttributePath> attributePaths = new HashMap<>();
		final Map<String, Attribute> attributes = new HashMap<>();

		if(contentSchema.getKeyAttributePaths() != null) {

			for(final AttributePath attributePath : contentSchema.getKeyAttributePaths()) {

				fillMaps(attributePath, attributePaths, attributes);
			}
		}

		if(contentSchema.getValueAttributePath() != null) {

			fillMaps(contentSchema.getValueAttributePath(), attributePaths, attributes);
		}

		final String commonPrefix = StringUtils.getCommonPrefix(attributePaths.keySet().toArray(new String[attributePaths.size()]));

		final String commonAttributePathString = cleanCommonPrefix(commonPrefix);

		if(attributePaths.containsKey(commonAttributePathString)) {

			return Optional.fromNullable(attributePaths.get(commonAttributePathString));
		}

		final String[] attributeURIs = commonAttributePathString.split(DMPStatics.ATTRIBUTE_DELIMITER.toString());

		final LinkedList<Attribute> apAttributes = new LinkedList<>();

		for(final String attributeURI : attributeURIs) {

			final Attribute attribute = attributes.get(attributeURI);
			apAttributes.add(attribute);
		}

		return Optional.of(new AttributePath(apAttributes));
	}

	public static LinkedList<AttributePath> parseAttributePathsNode(final JsonNode attributePathsNode) {

		return  parseAttributePathsNode(attributePathsNode, null, null);
	}

	public static LinkedList<AttributePath> parseAttributePathsNode(final JsonNode attributePathsNode, final Map<String, Attribute> attributeMap, final Map<String, AttributePath> attributePathMap) {

		if(attributePathsNode == null || !ArrayNode.class.isInstance(attributePathsNode)) {

			return null;
		}

		final LinkedList<AttributePath> attributePaths = new LinkedList<>();

		for(final JsonNode attributePathNode : attributePathsNode) {

			final AttributePath attributePath = parseAttributePathNode(attributePathNode, attributeMap, attributePathMap);

			if(attributePath != null) {

				attributePaths.add(attributePath);
			}
		}

		return attributePaths;
	}

	public static AttributePath parseAttributePathNode(final JsonNode attributePathNode) {

		return parseAttributePathNode(attributePathNode, null, null);
	}

	public static AttributePath parseAttributePathNode(final JsonNode attributePathNode, final Map<String, Attribute> attributeMap, final Map<String, AttributePath> attributePathMap) {

		if (attributePathNode == null) {

			return null;
		}

		final String attributePathString = attributePathNode.asText();

		return parseAttributePathString(attributePathString, attributeMap, attributePathMap);
	}

	public static AttributePath parseAttributePathString(final String attributePathString) {

		return parseAttributePathString(attributePathString, null, null);
	}

	public static AttributePath parseAttributePathString(final String attributePathString, final Map<String, Attribute> attributeMap, final Map<String, AttributePath> attributePathMap) {

		final String[] attributes = attributePathString.split(DMPStatics.ATTRIBUTE_DELIMITER.toString());

		if(attributes.length <= 0) {

			return null;
		}

		final LinkedList<Attribute> attributeList = new LinkedList<>();

		for (final String attributeURI : attributes) {

			final Attribute attribute = getOrCreateAttribute(attributeURI, Optional.fromNullable(attributeMap));
			attributeList.add(attribute);
		}

		return getOrCreateAttributePath(attributeList, Optional.fromNullable(attributePathMap));
	}

	private static Attribute getOrCreateAttribute(final String uri, final Optional<Map<String, Attribute>> optionalAttributeMap) {

		if(!optionalAttributeMap.isPresent()) {

			return new Attribute(uri);
		}

		if(!optionalAttributeMap.get().containsKey(uri)) {

			optionalAttributeMap.get().put(uri, new Attribute(uri));
		}

		return optionalAttributeMap.get().get(uri);
	}

	private static AttributePath getOrCreateAttributePath(final LinkedList<Attribute> attributePath, final Optional<Map<String, AttributePath>> optionalAttributePathMap) {

		if(!optionalAttributePathMap.isPresent()) {

			return new AttributePath(attributePath);
		}

		final String attributePathString = AttributePathUtil.generateAttributePath(attributePath);

		if(!optionalAttributePathMap.get().containsKey(attributePathString)) {

			optionalAttributePathMap.get().put(attributePathString, new AttributePath(attributePath));
		}

		return optionalAttributePathMap.get().get(attributePathString);
	}

	private static void fillMaps(final AttributePath attributePath, final Map<String, AttributePath> attributePaths,
			final Map<String, Attribute> attributes) {

		attributePaths.put(attributePath.toString(), attributePath);

		for (final Attribute apAttribute : attributePath.getAttributes()) {

			attributes.put(apAttribute.getUri(), apAttribute);
		}
	}

	private static String cleanCommonPrefix(final String commonPrefix) {

		if (!commonPrefix.endsWith(DMPStatics.ATTRIBUTE_DELIMITER.toString())) {

			if (!commonPrefix.contains(DMPStatics.ATTRIBUTE_DELIMITER.toString())) {

				return commonPrefix;
			}

			return commonPrefix.substring(0, commonPrefix.lastIndexOf(DMPStatics.ATTRIBUTE_DELIMITER));
		}

		return commonPrefix.substring(0, commonPrefix.length() - 1);
	}
}
