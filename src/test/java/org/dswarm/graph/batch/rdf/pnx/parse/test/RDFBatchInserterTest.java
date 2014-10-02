package org.dswarm.graph.batch.rdf.pnx.parse.test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dswarm.graph.batch.rdf.pnx.DataModelRDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.RDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.parse.DataModelRDFNeo4jHandler;
import org.dswarm.graph.batch.rdf.pnx.parse.PNXParser;
import org.dswarm.graph.batch.rdf.pnx.parse.RDFHandler;
import org.dswarm.graph.batch.rdf.pnx.parse.RDFParser;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

import de.knutwalker.ntparser.NonStrictNtParser;
import de.knutwalker.ntparser.Statement;

/**
 * @author tgaengler
 */
public class RDFBatchInserterTest {

	private static final Logger LOG = LoggerFactory.getLogger(RDFBatchInserterTest.class);
	
	@Test
	public void testRDFBatchInsertTest() throws Exception {

		LOG.debug("start batch processing");

		final String dataModelURI = "test";

		final Map<String, String> config = new HashMap<>();
		config.put("cache_type", "none");
		config.put("use_memory_mapped_buffers", "true");
		final BatchInserter inserter = BatchInserters.inserter("target/test_data", config);

		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(inserter, dataModelURI);
		final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);
		final RDFParser parser = new PNXParser(handler);

		LOG.debug("finished initializing batch inserter");

			LOG.debug("start batch import");

		final URL fileURL = Resources.getResource("dmpf_bsp1.nt");
		final byte[] file = Resources.toByteArray(fileURL);
		final InputStream stream = new ByteArrayInputStream(file);
		final Iterator<Statement> model = NonStrictNtParser.parse(stream);

		LOG.debug("finished loading RDF model");

		parser.parse(model);

		// flush indices etc.
		handler.getHandler().closeTransaction();

		LOG.debug("finished writing " + handler.getHandler().getCountedStatements() + " RDF statements ('"
				+ handler.getHandler().getRelationshipsAdded() + "' added relationships) into graph db for data model URI '" + dataModelURI
				+ "'");
		NonStrictNtParser.close();

		stream.close();
		inserter.shutdown();

		LOG.debug("shutdown batch inserter");
	}
}
