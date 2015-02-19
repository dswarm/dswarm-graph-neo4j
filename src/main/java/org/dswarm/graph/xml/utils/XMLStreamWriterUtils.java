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
package org.dswarm.graph.xml.utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.dswarm.common.web.URI;

/**
 * @author tgaengler
 */
public final class XMLStreamWriterUtils {

	private static final AtomicInteger counter = new AtomicInteger(0);
	private static final String NAMESPACE_PREFIX_BASE = "ns";

	public static void writeXMLElementTag(final XMLStreamWriter writer, final URI uri, final Map<String, String> namespacesPrefixesMap)
			throws XMLStreamException {

		if (uri.hasNamespaceURI()) {

			final String prefix = getPrefix(uri.getNamespaceURI().substring(0, uri.getNamespaceURI().length() - 1), namespacesPrefixesMap);

			writer.writeStartElement(prefix, uri.getLocalName(), uri.getNamespaceURI().substring(0, uri.getNamespaceURI().length() - 1));
			//writer.writeNamespace(prefix, uri.getNamespaceURI().substring(0, uri.getNamespaceURI().length() - 1));
		} else {

			writer.writeStartElement(uri.getLocalName());
		}
	}

	public static void writeXMLAttribute(final XMLStreamWriter writer, final URI uri, final String value,
			final Map<String, String> namespacesPrefixesMap) throws XMLStreamException {

		if (uri.hasNamespaceURI()) {

			final String prefix = getPrefix(uri.getNamespaceURI().substring(0, uri.getNamespaceURI().length() - 1), namespacesPrefixesMap);

			writer.writeAttribute(prefix, uri.getNamespaceURI().substring(0, uri.getNamespaceURI().length() - 1), uri.getLocalName(), value);
		} else {

			writer.writeAttribute(uri.getLocalName(), value);
		}
	}

	public static String getPrefix(final String namespace, final Map<String, String> namespacesPrefixesMap) {

		if (!namespacesPrefixesMap.containsKey(namespace)) {

			namespacesPrefixesMap.put(namespace, NAMESPACE_PREFIX_BASE + counter.incrementAndGet());
		}

		return namespacesPrefixesMap.get(namespace);
	}
}
