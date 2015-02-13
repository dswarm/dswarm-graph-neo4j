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
import org.dswarm.graph.model.util.AttributePathUtil;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

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
		final AttributePath recordIdentifierAttributePath = AttributePathUtil.parseAttributePathNode(recordIdentifierAttributePathNode, attributeMap,
				attributePathMap);

		final JsonNode keyAttributePathsNode = node.get("key_attribute_paths");
		final LinkedList<AttributePath> keyAttributePaths = AttributePathUtil.parseAttributePathsNode(keyAttributePathsNode, attributeMap,
				attributePathMap);

		final JsonNode valueAttributePathNode = node.get("value_attribute_path");
		final AttributePath valueAttributePath = AttributePathUtil.parseAttributePathNode(valueAttributePathNode, attributeMap, attributePathMap);

		return new ContentSchema(recordIdentifierAttributePath, keyAttributePaths, valueAttributePath);
	}


}
