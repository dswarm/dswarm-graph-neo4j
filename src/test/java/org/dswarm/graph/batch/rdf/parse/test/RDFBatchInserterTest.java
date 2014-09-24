package org.dswarm.graph.batch.rdf.parse.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * @author tgaengler
 */
public class RDFBatchInserterTest {

	private static final Logger LOG = LoggerFactory.getLogger(RDFBatchInserterTest.class);

	@Test
	public void testRDFBatchInsertTest() throws Exception {

		LOG.debug("start batch import");

		final String dataModelURI = "dnb";

		final Path path = Paths.get("/var/lib/neo4j/conf/neo4j.properties");
		final InputStream input = Files.newInputStream(path);
		final Map<String, String> config = MapUtil.load(input);
		input.close();
		final BatchInserter inserter = BatchInserters.inserter("target/batchinserter-example-config", config);

		LOG.debug("finish initializing batch inserter");

		final Path modelPath = Paths.get("/media/tgaengler/data/dnb_dump/nt/dnb_dump_000001.nt");
		//final Path modelPath = Paths.get("/home/tgaengler/git/dmp-graph/dmp-graph/src/test/resources/dmpf_bsp1.nt");
		final BufferedReader modelInput = Files.newBufferedReader(modelPath, Charsets.UTF_8);
		final Model model = ModelFactory.createDefaultModel();
		model.read(modelInput, null, "N3");

		LOG.debug("finished loading RDF model");

		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(inserter, dataModelURI);
		final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);
		final RDFParser parser = new JenaModelParser(model);
		parser.setRDFHandler(handler);
		parser.parse();

		// flush indices etc.
		handler.getHandler().closeTransaction();

		LOG.debug("finished writing " + handler.getHandler().getCountedStatements() + " RDF statements ('"
				+ handler.getHandler().getRelationshipsAdded() + "' added relationships) into graph db for data model URI '" + dataModelURI + "'");

		inserter.shutdown();
		modelInput.close();
	}
}
