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
package org.dswarm.graph.resources;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public abstract class GraphResource {

	private static final Logger LOG = LoggerFactory.getLogger(GraphResource.class);

	protected static final ObjectMapper simpleObjectMapper = new ObjectMapper();

	protected Optional<String> getStringValue(final String key, final JsonNode json) {

		final JsonNode node = json.get(key);
		final Optional<String> optionalValue;

		if (node != null) {

			optionalValue = Optional.ofNullable(node.asText());
		} else {

			optionalValue = Optional.empty();
		}

		return optionalValue;
	}

	protected Optional<Integer> getIntValue(final String key, final JsonNode json) {

		final JsonNode node = json.get(key);
		final Optional<Integer> optionalValue;

		if (node != null) {

			optionalValue = Optional.ofNullable(node.asInt());
		} else {

			optionalValue = Optional.empty();
		}

		return optionalValue;
	}

	protected ObjectNode deserializeJSON(final String jsonString, final String type) throws DMPGraphException {

		try {

			return simpleObjectMapper.readValue(jsonString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = String.format("could not deserialise request JSON for %s", type);

			GraphResource.LOG.error(message);

			throw new DMPGraphException(message, e);
		}
	}

	protected String serializeJSON(final Object object, final String type) throws DMPGraphException {

		try {

			return simpleObjectMapper.writeValueAsString(object);
		} catch (final JsonProcessingException e) {

			final String message = String.format("some problems occur, while processing the JSON for %s", type);

			GraphResource.LOG.error(message);

			throw new DMPGraphException(message, e);
		}
	}

	protected String readHeaders(final HttpHeaders httpHeaders) {

		final MultivaluedMap<String, String> headerParams = httpHeaders.getRequestHeaders();

		final StringBuilder sb = new StringBuilder();

		for (final Map.Entry<String, List<String>> entry : headerParams.entrySet()) {

			final String headerIdentifier = entry.getKey();
			final List<String> headerValues = entry.getValue();

			sb.append("\t\t").append(headerIdentifier).append(" = ");

			for (final String headerValue : headerValues) {

				sb.append(headerValue).append(", ");
			}

			sb.append("\n");
		}

		return sb.toString();
	}
}
