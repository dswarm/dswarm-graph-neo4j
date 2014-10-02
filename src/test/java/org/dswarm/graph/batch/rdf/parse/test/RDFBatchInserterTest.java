package org.dswarm.graph.batch.rdf.parse.test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.dswarm.graph.batch.rdf.DataModelRDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.RDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.parse.DataModelRDFNeo4jHandler;
import org.dswarm.graph.rdf.parse.JenaModelParser;
import org.dswarm.graph.rdf.parse.RDFHandler;
import org.dswarm.graph.rdf.parse.RDFParser;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * @author tgaengler
 */
public class RDFBatchInserterTest {

	private static final Logger LOG = LoggerFactory.getLogger(RDFBatchInserterTest.class);

	private static final String BASE_PATH = "/media/tgaengler/data/dnb_dump/nt/dnb_dump_0000";
	private static final String PATH_POSTFIX = ".nt";

	//@Test
	public void testRDFBatchInsertTest() throws Exception {

		LOG.debug("start batch processing");

		final String dataModelURI = "dnb";

		final Path path = Paths.get("/var/lib/neo4j/conf/neo4j.properties");
		final InputStream input = Files.newInputStream(path);
		final Map<String, String> config = MapUtil.load(input);
		input.close();
		final BatchInserter inserter = BatchInserters.inserter("/media/tgaengler/data/projects/ekz/neo4j/dnb_data", config);

		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(inserter, dataModelURI);
		final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);

		LOG.debug("finished initializing batch inserter");

		for (int i = 1; i < 33; i++) {

			LOG.debug("start batch import pt. " + i);

			final StringBuilder sb = new StringBuilder();

			sb.append(BASE_PATH);

			if(i < 10) {

				sb.append("0");
			}

			sb.append(i).append(PATH_POSTFIX);

			final Path modelPath = Paths.get(sb.toString());
					//final Path modelPath = Paths.get("/home/tgaengler/git/dmp-graph/dmp-graph/src/test/resources/dmpf_bsp1.nt");
			final BufferedReader modelInput = Files.newBufferedReader(modelPath, Charsets.UTF_8);
			final Model model = ModelFactory.createDefaultModel();
			model.read(modelInput, null, "N3");

			LOG.debug("finished loading RDF model");

			final RDFParser parser = new JenaModelParser(model);
			parser.setRDFHandler(handler);
			parser.parse();

			// flush indices etc.
			handler.getHandler().closeTransaction();

			LOG.debug("finished writing " + handler.getHandler().getCountedStatements() + " RDF statements ('"
					+ handler.getHandler().getRelationshipsAdded() + "' added relationships) into graph db for data model URI '" + dataModelURI
					+ "'");
			modelInput.close();
			model.close();
		}

		inserter.shutdown();

		LOG.debug("shutdown batch inserter");
	}
	
	@Test
	public void testRDFBatchInsertTest3() throws Exception {

		LOG.debug("start batch processing");

		final String dataModelURI = "test";

		final Map<String, String> config = new HashMap<>();
		config.put("cache_type", "none");
		config.put("use_memory_mapped_buffers", "true");
		final BatchInserter inserter = BatchInserters.inserter("target/test_data2", config);

		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(inserter, dataModelURI);
		final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);

		LOG.debug("finished initializing batch inserter");

			LOG.debug("start batch import");

		final URL fileURL = Resources.getResource("dmpf_bsp1.n3");
		final byte[] file = Resources.toByteArray(fileURL);
		final InputStream stream = new ByteArrayInputStream(file);
		final Model model = ModelFactory.createDefaultModel();
		model.read(stream, null, "N3");

		LOG.debug("finished loading RDF model");

		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		// flush indices etc.
			handler.getHandler().closeTransaction();

		LOG.debug("finished writing " + handler.getHandler().getCountedStatements() + " RDF statements ('"
				+ handler.getHandler().getRelationshipsAdded() + "' added relationships) into graph db for data model URI '" + dataModelURI
				+ "'");
		stream.close();
		model.close();

		inserter.shutdown();

		LOG.debug("shutdown batch inserter");
	}
}
