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
package org.dswarm.graph.model.deserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.dswarm.graph.model.Attribute;
import org.dswarm.graph.model.AttributePath;
import org.dswarm.graph.model.ContentSchema;
import org.dswarm.graph.model.DMPStatics;
import org.dswarm.graph.model.util.AttributePathUtil;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Created by tgaengler on 29/07/14.
 */
public class ContentSchemaDeserializer extends JsonDeserializer<ContentSchema> {

	private Map<String, Attribute> attributeMap = new HashMap<>();
	private Map<String, AttributePath> attributePathMap = new HashMap<>();

	@Override
	public ContentSchema deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException {

		final ObjectCodec oc = jp.getCodec();

		if (oc == null) {

			return null;
		}

		final JsonNode node = oc.readTree(jp);

		if (node == null) {

			return null;
		}

		final JsonNode recordIdentifierAttributePathNode = node.get("record_identifier_attribute_path");
		final AttributePath recordIdentifierAttributePath = parseAttributePathNode(recordIdentifierAttributePathNode);

		final JsonNode keyAttributePathsNode = node.get("key_attribute_paths");
		final LinkedList<AttributePath> keyAttributePaths = parseAttributePathsNode(keyAttributePathsNode);

		final JsonNode valueAttributePathNode = node.get("value_attribute_path");
		final AttributePath valueAttributePath = parseAttributePathNode(valueAttributePathNode);

		return new ContentSchema(recordIdentifierAttributePath, keyAttributePaths, valueAttributePath);
	}

	private LinkedList<AttributePath> parseAttributePathsNode(final JsonNode attributePathsNode) {

		if(attributePathsNode == null || !ArrayNode.class.isInstance(attributePathsNode)) {

			return null;
		}

		final LinkedList<AttributePath> attributePaths = new LinkedList<>();

		for(final JsonNode attributePathNode : attributePathsNode) {

			final AttributePath attributePath = parseAttributePathNode(attributePathNode);

			if(attributePath != null) {

				attributePaths.add(attributePath);
			}
		}

		return attributePaths;
	}

	private AttributePath parseAttributePathNode(final JsonNode attributePathNode) {

		if (attributePathNode == null) {

			return null;
		}

		final String attributePathString = attributePathNode.asText();

		return parseAttributePathString(attributePathString);
	}

	private AttributePath parseAttributePathString(final String attributePathString) {

		final String[] attributes = attributePathString.split(DMPStatics.ATTRIBUTE_DELIMITER.toString());

		if(attributes.length <= 0) {

			return null;
		}

		final LinkedList<Attribute> attributeList = new LinkedList<>();

		for (final String attributeURI : attributes) {

			final Attribute attribute = getOrCreateAttribute(attributeURI);
			attributeList.add(attribute);
		}

		return getOrCreateAttributePath(attributeList);
	}

	private Attribute getOrCreateAttribute(final String uri) {

		if(!attributeMap.containsKey(uri)) {

			attributeMap.put(uri, new Attribute(uri));
		}

		return attributeMap.get(uri);
	}

	private AttributePath getOrCreateAttributePath(final LinkedList<Attribute> attributePath) {

		final String attributePathString = AttributePathUtil.generateAttributePath(attributePath);

		if(!attributePathMap.containsKey(attributePathString)) {

			attributePathMap.put(attributePathString, new AttributePath(attributePath));
		}

		return attributePathMap.get(attributePathString);
	}
}
