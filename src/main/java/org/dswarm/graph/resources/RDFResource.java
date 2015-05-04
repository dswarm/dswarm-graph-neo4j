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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.MultiPart;
import de.knutwalker.ntparser.NonStrictNtParser;
import de.knutwalker.ntparser.model.NtModelFactory;
import de.knutwalker.ntparser.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.DMPStatics;
import org.dswarm.common.MediaTypeUtil;
import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.index.NamespaceIndex;
import org.dswarm.graph.rdf.DataModelRDFNeo4jProcessor;
import org.dswarm.graph.rdf.RDFNeo4jProcessor;
import org.dswarm.graph.rdf.SimpleRDFNeo4jProcessor;
import org.dswarm.graph.rdf.export.DataModelRDFExporter;
import org.dswarm.graph.rdf.export.GraphRDFExporter;
import org.dswarm.graph.rdf.export.RDFExporter;
import org.dswarm.graph.rdf.parse.DataModelRDFNeo4jHandler;
import org.dswarm.graph.rdf.parse.JenaModelParser;
import org.dswarm.graph.rdf.parse.RDFHandler;
import org.dswarm.graph.rdf.parse.RDFParser;
import org.dswarm.graph.rdf.parse.SimpleRDFNeo4jHandler;
import org.dswarm.graph.rdf.pnx.parse.PNXParser;
import org.dswarm.graph.rdf.read.PropertyGraphRDFReader;
import org.dswarm.graph.rdf.read.RDFReader;

/**
 * @author tgaengler
 */
@Path("/rdf")
public class RDFResource {

	private static final Logger LOG = LoggerFactory.getLogger(RDFResource.class);

	/**
	 * The object mapper that can be utilised to de-/serialise JSON nodes.
	 */
	private final ObjectMapper objectMapper;

	public RDFResource() {

		objectMapper = new ObjectMapper();
	}

	@GET
	@Path("/ping")
	public String ping() {

		RDFResource.LOG.debug("ping was called");

		return "pong";
	}

	@POST
	@Path("/put")
	@Consumes("multipart/mixed")
	public Response writeRDF(final MultiPart multiPart, @Context final GraphDatabaseService database) throws DMPGraphException, IOException {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		final String dataModelURI = multiPart.getBodyParts().get(0).getEntityAs(String.class);

		final BodyPartEntity bpe = (BodyPartEntity) multiPart.getBodyParts().get(1).getEntity();
		final InputStream rdfInputStream = bpe.getInputStream();

		final Model model = ModelFactory.createDefaultModel();
		model.read(rdfInputStream, null, "N3");

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as Turtle or N3");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final NamespaceIndex namespaceIndex = new NamespaceIndex(database);
		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(database, namespaceIndex, dataModelURI);

		try {

			final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);
			final RDFParser parser = new JenaModelParser(model);
			parser.setRDFHandler(handler);
			parser.parse();

			handler.getHandler().closeTransaction();
			rdfInputStream.close();

			LOG.debug(
					"finished writing {} RDF statements (added {} relationships, added {} nodes (resources + bnodes + literals), added {} literals) into graph db for data model URI '{}'",
					handler.getHandler().getCountedStatements(),
					handler.getHandler().getRelationshipsAdded(), handler.getHandler().getNodesAdded(), handler.getHandler().getCountedLiterals(),
					dataModelURI);
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			if (rdfInputStream != null) {

				rdfInputStream.close();
			}

			LOG.error("couldn't write RDF statements into graph db: " + e.getMessage(), e);

			throw e;
		}

		return Response.ok().build();
	}

	@POST
	@Path("/put")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeRDF(final InputStream inputStream, @Context final GraphDatabaseService database) throws DMPGraphException, IOException {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		final Model model = ModelFactory.createDefaultModel();
		model.read(inputStream, null, "N3");

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as Turtle and N3");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final NamespaceIndex namespaceIndex = new NamespaceIndex(database);
		final RDFNeo4jProcessor processor = new SimpleRDFNeo4jProcessor(database, namespaceIndex);

		try {

			final RDFHandler handler = new SimpleRDFNeo4jHandler(processor);
			final RDFParser parser = new JenaModelParser(model);
			parser.setRDFHandler(handler);
			parser.parse();

			handler.getHandler().closeTransaction();
			inputStream.close();

			RDFResource.LOG.debug(
					"finished writing {} RDF statements (added {} relationships, added {} nodes (resources + bnodes + literals), added {} literals) into graph db",
					handler.getHandler().getCountedStatements(),
					handler.getHandler().getRelationshipsAdded(), handler.getHandler().getNodesAdded(), handler.getHandler().getCountedLiterals());
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			if (inputStream != null) {

				inputStream.close();
			}

			LOG.error("couldn't write RDF statements into graph db: " + e.getMessage(), e);

			throw e;
		}

		return Response.ok().build();
	}

	@POST
	@Path("/putpnx")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response writeRDFwPNX(final InputStream inputStream, @Context final GraphDatabaseService database) throws DMPGraphException, IOException {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		if (inputStream == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there is no input stream");
		}

		final InputStream in = new BufferedInputStream(inputStream, 1024);

		final Iterator<Statement> model = NonStrictNtParser.parse(in, NtModelFactory.INSTANCE());

		if (model == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there are no statements");
		}

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as N-Triples");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final NamespaceIndex namespaceIndex = new NamespaceIndex(database);
		final org.dswarm.graph.rdf.pnx.RDFNeo4jProcessor processor = new org.dswarm.graph.rdf.pnx.SimpleRDFNeo4jProcessor(database, namespaceIndex);

		try {

			final org.dswarm.graph.rdf.pnx.parse.RDFHandler handler = new org.dswarm.graph.rdf.pnx.parse.SimpleRDFNeo4jHandler(processor);
			final org.dswarm.graph.rdf.pnx.parse.RDFParser parser = new PNXParser(handler);
			parser.parse(model);

			handler.getHandler().closeTransaction();
			in.close();
			inputStream.close();

			RDFResource.LOG.debug(
					"finished writing {} RDF statements (added {} relationships, added {} nodes (resources + bnodes + literals), added {} literals) into graph db",
					handler.getHandler().getCountedStatements(),
					handler.getHandler().getRelationshipsAdded(), handler.getHandler().getNodesAdded(), handler.getHandler().getCountedLiterals());
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			in.close();
			inputStream.close();

			LOG.error("couldn't write RDF statements into graph db: {}", e.getMessage(), e);

			throw e;
		}

		return Response.ok().build();
	}

	@POST
	@Path("/putpnx")
	@Consumes("multipart/mixed")
	public Response writeRDFwDataModelwPNX(final MultiPart multiPart, @Context final GraphDatabaseService database)
			throws DMPGraphException, IOException {

		RDFResource.LOG.debug("try to process RDF statements and write them into graph db");

		if (multiPart == null) {

			throw new DMPGraphException("couldn't process RDF statements, because the mutlipart payload is not available");
		}

		final List<BodyPart> bodyParts = multiPart.getBodyParts();

		if (bodyParts == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there are no bodyparts in the multipart payload");
		}

		final BodyPart dataModelURIBodyPart = bodyParts.get(0);

		if (dataModelURIBodyPart == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there is no data model URI body part");
		}

		final String dataModelURI = dataModelURIBodyPart.getEntityAs(String.class);

		final BodyPart inputSteamBodyPart = bodyParts.get(1);

		if (inputSteamBodyPart == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there is no input stream body part");
		}

		final BodyPartEntity inputStreamBodyPartEntity = (BodyPartEntity) inputSteamBodyPart.getEntity();

		if (inputStreamBodyPartEntity == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there is no input stream body part entity");
		}

		final InputStream rdfInputStream = inputStreamBodyPartEntity.getInputStream();

		if (rdfInputStream == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there is no input stream");
		}

		final InputStream in = new BufferedInputStream(rdfInputStream, 1024);

		final Iterator<Statement> model = NonStrictNtParser.parse(in, NtModelFactory.INSTANCE());

		if (model == null) {

			throw new DMPGraphException("couldn't process RDF statements, because there are no statements");
		}

		RDFResource.LOG.debug("deserialized RDF statements that were serialised as N-Triples");

		RDFResource.LOG.debug("try to write RDF statements into graph db");

		final NamespaceIndex namespaceIndex = new NamespaceIndex(database);
		final org.dswarm.graph.rdf.pnx.RDFNeo4jProcessor processor = new org.dswarm.graph.rdf.pnx.DataModelRDFNeo4jProcessor(database, namespaceIndex, dataModelURI);

		try {

			final org.dswarm.graph.rdf.pnx.parse.RDFHandler handler = new org.dswarm.graph.rdf.pnx.parse.DataModelRDFNeo4jHandler(processor);
			final org.dswarm.graph.rdf.pnx.parse.RDFParser parser = new PNXParser(handler);
			parser.parse(model);

			handler.getHandler().closeTransaction();
			in.close();
			rdfInputStream.close();

			LOG.debug(
					"finished writing {} RDF statements (added {} relationships, added {} nodes (resources + bnodes + literals), added {} literals) into graph db for data model URI '{}'",
					handler.getHandler().getCountedStatements(),
					handler.getHandler().getRelationshipsAdded(), handler.getHandler().getNodesAdded(), handler.getHandler().getCountedLiterals(),
					dataModelURI);
		} catch (final Exception e) {

			processor.getProcessor().failTx();

			in.close();
			rdfInputStream.close();

			LOG.error("couldn't write RDF statements into graph db: {}", e.getMessage(), e);

			throw e;
		}

		return Response.ok().build();
	}

	@POST
	@Path("/get")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces("application/n-triples")
	public Response readRDF(final String jsonObjectString, @Context final GraphDatabaseService database) throws DMPGraphException {

		RDFResource.LOG.debug("try to read RDF statements from graph db");

		final ObjectNode json;

		try {

			json = objectMapper.readValue(jsonObjectString, ObjectNode.class);
		} catch (final IOException e) {

			final String message = "could not deserialise request JSON for read from graph DB request";

			RDFResource.LOG.debug(message);

			throw new DMPGraphException(message, e);
		}

		final String recordClassUri = json.get(DMPStatics.RECORD_CLASS_URI_IDENTIFIER).asText();
		final String dataModelUri = json.get(DMPStatics.DATA_MODEL_URI_IDENTIFIER).asText();

		LOG.debug("try to read RDF statements for data model uri = '" + dataModelUri + "' and record class uri = '" + recordClassUri
				+ "' from graph db");

		final RDFReader rdfReader = new PropertyGraphRDFReader(recordClassUri, dataModelUri, database);
		final Model model = rdfReader.read();

		// model.write(System.out, "N-TRIPLE");

		final StringWriter writer = new StringWriter();
		model.write(writer, "N-TRIPLE");
		final String result = writer.toString();

		LOG.debug("finished reading '" + model.size() + "' RDF statements ('" + rdfReader.countStatements()
				+ "' via RDF reader) for data model uri = '" + dataModelUri + "' and record class uri = '" + recordClassUri + "' from graph db");

		return Response.ok().entity(result).build();
	}

	/**
	 * for triggering a download of all data models. The serialization (export) format is provided via the accept header field. If
	 * no format is provided, {@link MediaTypeUtil#N_QUADS} is used as default. In case the format is not supported a 406 response
	 * is sent.
	 *
	 * @param database the db to export the data from
	 * @param exportFormat serialization format all data should be serialized in, injected from accept header field
	 * @return all data models serialized in exportLanguage
	 * @throws DMPGraphException in case exportFormat can not be converted to {@link MediaType}
	 */
	@GET
	// SR TODO rename to /exportall
	@Path("/getall")
	@Produces({ MediaTypeUtil.N_QUADS, MediaTypeUtil.TRIG })
	public Response exportAllRDFForDownload(@Context final GraphDatabaseService database,
			@HeaderParam("Accept") @DefaultValue(MediaTypeUtil.N_QUADS) final String exportFormat) throws DMPGraphException {

		RDFResource.LOG.debug("Start processing request to export all rdf data to format \"" + exportFormat + "\"");

		final MediaType formatType;
		try {
			formatType = MediaType.valueOf(exportFormat);
		} catch (IllegalArgumentException wrongFormat) {
			RDFResource.LOG.debug("Requested format \"" + exportFormat + "\" can not be used to create a MediaType. See exception: "
					+ wrongFormat.getLocalizedMessage());
			throw new DMPGraphException(wrongFormat.getLocalizedMessage());
		}

		// determine export language and file extension
		final Lang exportLanguage = RDFLanguages.contentTypeToLang(formatType.toString());
		final String fileExtension = exportLanguage.getFileExtensions().get(0);
		RDFResource.LOG.debug("Exporting rdf data to " + formatType.toString());

		final String result = exportAllRDFInternal(database, exportLanguage);

		RDFResource.LOG.debug("End processing request to export all rdf data to format \"" + exportFormat + "\"");

		return Response.ok(result).type(formatType.toString())
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export." + fileExtension).build();
	}

	/**
	 * trigger a download for a given data model and format. The serialization (export) format is provided via the accept header
	 * field. If no format is provided, {@link MediaTypeUtil#N_QUADS} is used as default. In case the format is not supported a
	 * 406 response is sent.
	 *
	 * @param database the graph database
	 * @param exportFormat serialization format ({@link MediaType}) the data model should be serialized in, injected from accept
	 *            header field
	 * @param dataModelURI the data model to be exported
	 * @return a single data model, serialized in exportLanguage
	 * @throws DMPGraphException in case exportFormat can not be converted to {@link MediaType}
	 */
	@GET
	@Path("/export")
	@Produces({ MediaTypeUtil.N_QUADS, MediaTypeUtil.RDF_XML, MediaTypeUtil.TRIG, MediaTypeUtil.TURTLE, MediaTypeUtil.N3 })
	public Response exportSingleRDFForDownload(@Context final GraphDatabaseService database,
			@HeaderParam("Accept") @DefaultValue(MediaTypeUtil.N_QUADS) final String exportFormat,
			@QueryParam("data_model_uri") final String dataModelURI) throws DMPGraphException {

		RDFResource.LOG.debug("Start processing request to export rdf data for data model uri \"" + dataModelURI + "\" to format \"" + exportFormat
				+ "\"");

		// determine export language and file extension
		final MediaType formatType;
		try {
			formatType = MediaType.valueOf(exportFormat);
		} catch (IllegalArgumentException wrongFormat) {
			// SR TODO remove log (antipattern log+throw)
			RDFResource.LOG.debug("Requested format \"" + exportFormat + "\" can not be used to create a MediaType. See exception: "
					+ wrongFormat.getLocalizedMessage());
			throw new DMPGraphException(wrongFormat.getLocalizedMessage());
		}
		final Lang exportLanguage = RDFLanguages.contentTypeToLang(formatType.toString());
		final String fileExtension = exportLanguage.getFileExtensions().get(0);
		RDFResource.LOG.debug("Interpreting requested format \"" + exportFormat + "\" as \"" + formatType.toString() + "\"");

		// export and serialize data
		final String result = exportSingleRDFInternal(database, exportLanguage, dataModelURI);

		RDFResource.LOG.debug("End processing request to export rdf data for data model uri \"" + dataModelURI + "\" to format \"" + exportFormat
				+ "\"");

		return Response.ok(result).type(formatType.toString())
				.header("Content-Disposition", "attachment; filename*=UTF-8''rdf_export." + fileExtension).build();
	}

	/**
	 * @param database the db to export the data from
	 * @param exportLanguage the language the data should be serialized in
	 * @param dataModelURI db internal identifier of the data model
	 * @return a single data model, serialized in exportLanguage
	 */
	private String exportSingleRDFInternal(final GraphDatabaseService database, final Lang exportLanguage, final String dataModelURI)
			throws DMPGraphException {

		RDFResource.LOG.debug("try to export all RDF statements for dataModelURI \"" + dataModelURI + "\" from graph db to format \""
				+ exportLanguage.getLabel() + "\"");

		// get data from neo4j
		final RDFExporter rdfExporter = new DataModelRDFExporter(database, dataModelURI);
		final Dataset dataset = rdfExporter.export();
		final Model exportedModel = dataset.getNamedModel(dataModelURI);

		// serialize (export) model to exportLanguage
		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, exportedModel, exportLanguage);
		final String result = writer.toString();

		RDFResource.LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		// LOG.debug("exported result:\n" + result);

		return result;
	}

	/**
	 * @param database the db to export the data from
	 * @param exportLanguage the language all data should be serialized in
	 * @return all data models serialized in exportLanguage
	 */
	private String exportAllRDFInternal(final GraphDatabaseService database, final Lang exportLanguage) throws DMPGraphException {

		RDFResource.LOG.debug("try to export all RDF statements (one graph = one data resource/model) from graph db");

		// get data from neo4j
		final RDFExporter rdfExporter = new GraphRDFExporter(database);
		final Dataset dataset = rdfExporter.export();

		// serialize (export) model to exportLanguage
		final StringWriter writer = new StringWriter();
		RDFDataMgr.write(writer, dataset, exportLanguage);
		final String result = writer.toString();

		RDFResource.LOG.debug("finished exporting " + rdfExporter.countStatements() + " RDF statements from graph db (processed statements = '"
				+ rdfExporter.processedStatements() + "' (successfully processed statements = '" + rdfExporter.successfullyProcessedStatements()
				+ "'))");

		return result;
	}

}
