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
package org.dswarm.graph.xml.read;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.codehaus.stax2.XMLOutputFactory2;

import org.dswarm.common.DMPStatics;
import org.dswarm.common.model.Attribute;
import org.dswarm.common.model.AttributePath;
import org.dswarm.common.types.Tuple;
import org.dswarm.common.web.URI;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.GraphIndexStatics;
import org.dswarm.graph.Neo4jProcessor;
import org.dswarm.graph.gdm.read.PropertyGraphGDMReaderHelper;
import org.dswarm.graph.hash.HashUtils;
import org.dswarm.graph.json.LiteralNode;
import org.dswarm.graph.json.NodeType;
import org.dswarm.graph.json.Predicate;
import org.dswarm.graph.model.GraphStatics;
import org.dswarm.graph.versioning.Range;
import org.dswarm.graph.versioning.VersioningStatics;
import org.dswarm.graph.versioning.utils.GraphVersionUtils;
import org.dswarm.graph.xml.utils.XMLStreamWriterUtils;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.lambdaj.Lambda;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * @author tgaengler
 */
public class PropertyGraphXMLReader implements XMLReader {

	private static final Logger							LOG						= LoggerFactory.getLogger(PropertyGraphXMLReader.class);

	/**
	 * TODO: shall we produce XML 1.0 or XML 1.1?
	 */
	private static final String							XML_VERSION				= "1.0";
	private static final XMLOutputFactory2				xmlOutputFactory;

	static {

		System.setProperty("javax.xml.stream.XMLOutputFactory", "com.fasterxml.aalto.stax.OutputFactoryImpl");

		xmlOutputFactory = (XMLOutputFactory2) XMLOutputFactory.newFactory();
		xmlOutputFactory.configureForSpeed();
	}

	private final String								dataModelUri;
	private final String								recordClassURIString;
	private final URI									recordClassURI;
	private final URI									recordTagURI;
	private final Optional<AttributePath> optionalRootAttributePath;

	private final Map<String, Tuple<Predicate, URI>> predicates            = new HashMap<>();
	private final Map<String, String>                namespacesPrefixesMap = new HashMap<>();
	private final Map<String, String>                nameMap               = new HashMap<>();

	private final GraphDatabaseService database;

	private long recordCount = 0;

	private Integer version;

	private final boolean originalDataTypeIsXML;

	private boolean isElementOpen = false;

	private Transaction tx = null;

	public PropertyGraphXMLReader(final Optional<AttributePath> optionalRootAttributePathArg, final Optional<String> optionalRecordTagArg,
			final String recordClassUriArg, final String dataModelUriArg, final Integer versionArg, final Optional<String> optionalOriginalDataType,
			final GraphDatabaseService databaseArg) throws DMPGraphException {

		optionalRootAttributePath = optionalRootAttributePathArg;
		recordClassURIString = recordClassUriArg;
		recordClassURI = new URI(recordClassURIString);

		if (optionalRecordTagArg.isPresent()) {

			recordTagURI = new URI(optionalRecordTagArg.get());
		} else {

			// record class URI as fall back

			recordTagURI = new URI(recordClassUriArg);
		}

		dataModelUri = dataModelUriArg;
		database = databaseArg;

		if (versionArg != null) {

			version = versionArg;
		} else {

			tx = database.beginTx();

			PropertyGraphXMLReader.LOG.debug("start read XML TX");

			try {

				version = GraphVersionUtils.getLatestVersion(dataModelUri, database);
			} catch (final Exception e) {

				final String message = "couldn't retrieve latest version successfully";

				PropertyGraphXMLReader.LOG.error(message, e);
				PropertyGraphXMLReader.LOG.debug("couldn't finish read XML TX successfully");

				tx.failure();
				tx.close();

				throw new DMPGraphException(message);
			}
		}

		originalDataTypeIsXML = optionalOriginalDataType.isPresent() && DMPStatics.XML_DATA_TYPE.equals(optionalOriginalDataType.get());
	}

	@Override
	public Optional<XMLStreamWriter> read(final OutputStream outputStream) throws DMPGraphException, XMLStreamException {

		if (tx == null) {

			try {

				PropertyGraphXMLReader.LOG.debug("start read XML TX");

				tx = database.beginTx();
			} catch (final Exception e) {

				final String message = "couldn't acquire tx successfully";

				PropertyGraphXMLReader.LOG.error(message, e);
				PropertyGraphXMLReader.LOG.debug("couldn't finish read XML TX successfully");

				throw new DMPGraphException(message);
			}
		}

		ResourceIterator<Node> recordNodesIter = null;

		try {

			final Label recordClassLabel = DynamicLabel.label(recordClassURIString);

			// TODO: refactor this to #findNodes + something else, then counting over the iterator
			final ResourceIterable<Node> recordNodes = database.findNodesByLabelAndProperty(recordClassLabel, GraphStatics.DATA_MODEL_PROPERTY,
					dataModelUri);

			if (recordNodes == null) {

				tx.success();

				PropertyGraphXMLReader.LOG.debug("there are no root nodes for '" + recordClassLabel + "' in data model '" + dataModelUri
						+ "'finished read XML TX successfully");

				return Optional.absent();
			}

			final int recordsSize = Iterables.size(recordNodes);

			recordNodesIter = recordNodes.iterator();

			if (recordNodesIter == null) {

				tx.success();

				PropertyGraphXMLReader.LOG.debug("there are no root nodes for '" + recordClassLabel + "' in data model '" + dataModelUri
						+ "'finished read XML TX successfully");

				return Optional.absent();
			}

			if (!recordNodesIter.hasNext()) {

				recordNodesIter.close();
				tx.success();

				PropertyGraphXMLReader.LOG.debug("there are no root nodes for '" + recordClassLabel + "' in data model '" + dataModelUri
						+ "'finished read XML TX successfully");

				return Optional.absent();
			}

			// (XMLStreamWriter2)
			final XMLStreamWriter writer = xmlOutputFactory.createXMLStreamWriter(outputStream);

			writer.writeStartDocument(Charsets.UTF_8.toString(), XML_VERSION);

			boolean defaultNamespaceWritten = false;

			if (optionalRootAttributePath.isPresent()) {

				// open root attribute path tags

				for (final Attribute attribute : optionalRootAttributePath.get().getAttributes()) {

					final URI attributeURI = new URI(attribute.getUri());

					if (!defaultNamespaceWritten && attributeURI.hasNamespaceURI()) {

						// set default namespace

						writer.setDefaultNamespace(attributeURI.getNamespaceURI());

						defaultNamespaceWritten = true;
					}

					XMLStreamWriterUtils.writeXMLElementTag(writer, attributeURI, namespacesPrefixesMap, nameMap, isElementOpen);
					isElementOpen = true;
				}
			} else if (recordsSize > 1) {

				// write default root
				final URI defaultRootURI = new URI(recordTagURI.toString() + "s");

				determineAndWriteXMLElementAndNamespace(defaultRootURI, writer);
			}

			if (!defaultNamespaceWritten && recordTagURI.hasNamespaceURI()) {

				// set default namespace
				setDefaultNamespace(writer);
			}

			final XMLRelationshipHandler relationshipHandler;

			if (originalDataTypeIsXML) {

				relationshipHandler = new CBDRelationshipXMLDataModelHandler(writer);
			} else {

				relationshipHandler = new CBDRelationshipHandler(writer);
			}

			// note: relationship handler knows this node handler
			new CBDNodeHandler(relationshipHandler);
			final XMLNodeHandler startNodeHandler = new CBDStartNodeHandler(relationshipHandler);

			// iterate over the records
			while (recordNodesIter.hasNext()) {

				final Node recordNode = recordNodesIter.next();
				final String resourceUri = (String) recordNode.getProperty(GraphStatics.URI_PROPERTY, null);

				if (resourceUri == null) {

					LOG.debug("there is no resource URI at record node '" + recordNode.getId() + "'");

					continue;
				}

				determineAndWriteXMLElementAndNamespace(recordTagURI, writer);

				startNodeHandler.handleNode(recordNode);
				// close record
				writer.writeEndElement();
				isElementOpen = false;

				recordCount++;
			}

			recordNodesIter.close();
			tx.success();

			PropertyGraphXMLReader.LOG.debug("finished read XML TX successfully");

			if (optionalRootAttributePath.isPresent()) {

				// close root attribute path tags

				for (int i = 0; i < optionalRootAttributePath.get().getAttributes().size(); i++) {

					writer.writeEndElement();
				}
			} else if (recordsSize > 1) {

				// close default root
				writer.writeEndElement();
			}

			// close document
			writer.writeEndDocument();

			return Optional.of(writer);
		} catch (final Exception e) {

			PropertyGraphXMLReader.LOG.error("couldn't finished read XML TX successfully", e);

			if (recordNodesIter != null) {

				recordNodesIter.close();
			}

			tx.failure();
		} finally {

			PropertyGraphXMLReader.LOG.debug("finished read GDM TX finally");

			tx.close();
		}

		return Optional.absent();
	}

	private void setDefaultNamespace(final XMLStreamWriter writer) throws XMLStreamException {

		// TODO: shall we cut the last character?

		final String defaultNameSpace;

		if (recordTagURI.hasNamespaceURI()) {

			defaultNameSpace = XMLStreamWriterUtils.determineBaseURI(recordTagURI);
		} else {

			defaultNameSpace = XMLStreamWriterUtils.determineBaseURI(recordClassURI);
		}

		writer.setDefaultNamespace(defaultNameSpace);
	}

	private URI determineAndWriteXMLElementAndNamespace(final URI uri, final XMLStreamWriter writer) throws XMLStreamException {

		final String prefix;
		final String namespace;
		final String finalURIString;
		final boolean namespaceAlreadySet;

		if (uri.hasNamespaceURI()) {

			namespace = XMLStreamWriterUtils.determineBaseURI(uri);
			namespaceAlreadySet = namespacesPrefixesMap.containsKey(namespace);
			prefix = XMLStreamWriterUtils.getPrefix(namespace, namespacesPrefixesMap);

			finalURIString = uri.getNamespaceURI() + uri.getLocalName();
		} else {

			namespace = XMLStreamWriterUtils.determineBaseURI(recordClassURI);
			namespaceAlreadySet = namespacesPrefixesMap.containsKey(namespace);
			prefix = XMLStreamWriterUtils.getPrefix(namespace, namespacesPrefixesMap);

			finalURIString = recordClassURI.getNamespaceURI() + uri.getLocalName();
		}

		final URI finalURI = new URI(finalURIString);

		// open record XML tag
		XMLStreamWriterUtils.writeXMLElementTag(writer, finalURI, namespacesPrefixesMap, nameMap, isElementOpen);
		isElementOpen = true;
		// TODO: shall we cut the last character?
		// TODO: shall we write the default namespace?
		// writer.writeDefaultNamespace(recordTagURI.getNamespaceURI().substring(0,
		// recordTagURI.getNamespaceURI().length() - 1));

		if (!namespaceAlreadySet) {

			writer.writeNamespace(prefix, namespace);
		}

		return finalURI;
	}

	@Override
	public long recordCount() {

		return recordCount;
	}

	private class CBDNodeHandler implements XMLNodeHandler {

		private final XMLRelationshipHandler	relationshipHandler;

		protected CBDNodeHandler(final XMLRelationshipHandler relationshipHandlerArg) {

			relationshipHandler = relationshipHandlerArg;
			((CBDRelationshipHandler) relationshipHandler).setNodeHandler(this);
		}

		@Override
		public void handleNode(final Node node) throws DMPGraphException, XMLStreamException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// node that holds the uri of the resource (record)
			// => maybe we should find an appropriated cypher query as replacement for this processing
			if (!node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				if (relationships == null || !relationships.iterator().hasNext()) {

					return;
				}

				// sort rels by index value
				// TODO: what should we do, if index is null (currently, the case for import via RDF)
				final List<Relationship> sortedRels = Lambda.sort(relationships,
						Lambda.on(Relationship.class).getProperty(GraphStatics.INDEX_PROPERTY));

				for (final Relationship relationship : sortedRels) {

					final Integer validFrom = (Integer) relationship.getProperty(VersioningStatics.VALID_FROM_PROPERTY, null);
					final Integer validTo = (Integer) relationship.getProperty(VersioningStatics.VALID_TO_PROPERTY, null);

					if (validFrom != null && validTo != null) {

						if (Range.range(validFrom, validTo).contains(version)) {

							relationshipHandler.handleRelationship(relationship);
						}
					} else {

						// TODO: remove this later, when every stmt is versioned
						relationshipHandler.handleRelationship(relationship);
					}
				}
			}
		}
	}

	private class CBDStartNodeHandler implements XMLNodeHandler {

		private final XMLRelationshipHandler	relationshipHandler;

		protected CBDStartNodeHandler(final XMLRelationshipHandler relationshipHandlerArg) {

			relationshipHandler = relationshipHandlerArg;
		}

		@Override
		public void handleNode(final Node node) throws DMPGraphException, XMLStreamException {

			// TODO: find a better way to determine the end of a resource description, e.g., add a property "resource" to each
			// (this is the case for model that came as GDM JSON)
			// node that holds the uri of the resource (record)
			if (node.hasProperty(GraphStatics.URI_PROPERTY)) {

				final Iterable<Relationship> relationships = node.getRelationships(Direction.OUTGOING);

				if (relationships == null || !relationships.iterator().hasNext()) {

					return;
				}

				// sort rels by index value
				// TODO: what should we do, if index is null (currently, the case for import via RDF)
				final List<Relationship> sortedRels = Lambda.sort(relationships,
						Lambda.on(Relationship.class).getProperty(GraphStatics.INDEX_PROPERTY));

				for (final Relationship relationship : sortedRels) {

					final Integer validFrom = (Integer) relationship.getProperty(VersioningStatics.VALID_FROM_PROPERTY, null);
					final Integer validTo = (Integer) relationship.getProperty(VersioningStatics.VALID_TO_PROPERTY, null);

					if (validFrom != null && validTo != null) {

						if (Range.range(validFrom, validTo).contains(version)) {

							relationshipHandler.handleRelationship(relationship);
						}
					} else {

						// TODO: remove this later, when every stmt is versioned
						relationshipHandler.handleRelationship(relationship);
					}
				}
			}
		}
	}

	/**
	 * Default handling: don't export RDF types and write literal objects as XML elements.
	 */
	protected class CBDRelationshipHandler implements XMLRelationshipHandler {

		private final PropertyGraphGDMReaderHelper propertyGraphGDMReaderHelper = new PropertyGraphGDMReaderHelper();

		protected final XMLStreamWriter writer;
		private         XMLNodeHandler  nodeHandler;

		protected CBDRelationshipHandler(final XMLStreamWriter writerArg) {

			writer = writerArg;
		}

		protected void setNodeHandler(final XMLNodeHandler nodeHandlerArg) {

			nodeHandler = nodeHandlerArg;
		}

		@Override
		public void handleRelationship(final Relationship rel) throws DMPGraphException, XMLStreamException {

			// note: we can also optionally check for the "resource property at the relationship (this property will only be
			// written right now for model that came as GDM JSON)
			if (rel.getProperty(GraphStatics.DATA_MODEL_PROPERTY).equals(dataModelUri)) {

				// subject => start element (???)

				final Node subjectNode = rel.getStartNode();
				final org.dswarm.graph.json.Node subjectGDMNode = propertyGraphGDMReaderHelper.readSubject(subjectNode);
				// => TODO, we need to compare the node, with the previous node, to write the content
				// (key(predicate)/value(object)) into the current element or another of this tag
				// TODO: how to determine, when we should close a tag (or parent tag etc.) => we need to keep a stack, of open
				// elements

				// predicate => XML element or XML attribute

				final String predicateString = rel.getType().name();
				final Tuple<Predicate, URI> predicateTuple = getPredicate(predicateString);
				final URI predicateURI = predicateTuple.v2();

				// object => XML Element value or XML attribute value or further recursion

				final Node objectNode = rel.getEndNode();
				final org.dswarm.graph.json.Node objectGDMNode = propertyGraphGDMReaderHelper.readObject(objectNode);

				writeKeyValue(predicateURI, objectGDMNode);

				// note: we can only iterate deeper into one direction, i.e., we need to cut the stream, when the object is
				// another resource => i.e. we iterate only when object are bnodes
				// TODO: what should we do with objects that are resources?
				if (objectGDMNode.getType().equals(NodeType.BNode)) {

					// open tag
					XMLStreamWriterUtils.writeXMLElementTag(writer, predicateURI, namespacesPrefixesMap, nameMap, isElementOpen);
					isElementOpen = true;

					// continue traversal with object node
					nodeHandler.handleNode(rel.getEndNode());

					// close
					writer.writeEndElement();
					isElementOpen = false;
				}
			}
		}

		protected void writeKeyValue(final URI predicateURI, final org.dswarm.graph.json.Node objectGDMNode) throws XMLStreamException {

			// default handling: don't export RDF types and write literal objects as XML elements
			if (!RDF.type.getURI().equals(predicateURI.toString()) && NodeType.Literal.equals(objectGDMNode.getType())) {

				// open tag
				XMLStreamWriterUtils.writeXMLElementTag(writer, predicateURI, namespacesPrefixesMap, nameMap, isElementOpen);

				writer.writeCData(((LiteralNode) objectGDMNode).getValue());

				// close
				writer.writeEndElement();
				isElementOpen = false;
			} else {

				// TODO: ???
			}
		}
	}

	/**
	 * Treat non-rdf:value/non-rdf:type statements with literal objects as XML attributes and rdf:value statements with literal
	 * objects as XML elements.
	 */
	private class CBDRelationshipXMLDataModelHandler extends CBDRelationshipHandler {

		protected CBDRelationshipXMLDataModelHandler(final XMLStreamWriter writerArg) {

			super(writerArg);
		}

		@Override
		protected void writeKeyValue(final URI predicateURI, final org.dswarm.graph.json.Node objectGDMNode) throws XMLStreamException {

			if (!(RDF.type.getURI().equals(predicateURI.toString()) || RDF.value.getURI().equals(predicateURI.toString()))
					&& NodeType.Literal.equals(objectGDMNode.getType())) {

				// predicate is an XML Attribute => write XML Attribute to this XML Element

				XMLStreamWriterUtils
						.writeXMLAttribute(writer, predicateURI, ((LiteralNode) objectGDMNode).getValue(), namespacesPrefixesMap, nameMap);
			} else if (RDF.value.getURI().equals(predicateURI.toString()) && NodeType.Literal.equals(objectGDMNode.getType())) {

				// predicate is an XML Element

				// TODO: what should we do with objects that are resources?
				writer.writeCData(((LiteralNode) objectGDMNode).getValue());
			} else {

				// ??? - log these occurrences?
			}
		}
	}

	private Tuple<Predicate, URI> getPredicate(final String predicateString) {

		if (!predicates.containsKey(predicateString)) {

			predicates.put(predicateString, Tuple.tuple(new Predicate(predicateString), new URI(predicateString)));
		}

		return predicates.get(predicateString);
	}
}
