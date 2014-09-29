package org.dswarm.graph.batch.rdf.pnx.parse.test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import de.knutwalker.ntparser.NonStrictNtParser;
import de.knutwalker.ntparser.Statement;

/**
 * @author tgaengler
 */
public class RDFBatchInserterTest {

	private static final Logger LOG = LoggerFactory.getLogger(RDFBatchInserterTest.class);

	private static final String BASE_PATH    = "/media/tgaengler/data/dnb_dump/nt/dnb_dump_0000";
	private static final String PATH_POSTFIX = ".nt";

	@Test
	public void testRDFBatchInsertTest() throws Exception {

		LOG.debug("start batch processing");

		final String dataModelURI = "dnb";

		final Path path = Paths.get("/var/lib/neo4j/conf/neo4j.properties");
		final InputStream input = Files.newInputStream(path);
		final Map<String, String> config = MapUtil.load(input);
		config.put("cache_type", "none");
		config.put("use_memory_mapped_buffers", "true");
		input.close();
		final BatchInserter inserter = BatchInserters.inserter("/media/tgaengler/data/projects/ekz/neo4j/dnb_data", config);

		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(inserter, dataModelURI);
		final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);
		final RDFParser parser = new PNXParser(handler);

		LOG.debug("finished initializing batch inserter");

		for (int i = 1; i < 33; i++) {

			LOG.debug("start batch import pt. " + i);

			final StringBuilder sb = new StringBuilder();

			sb.append(BASE_PATH);

			if (i < 10) {

				sb.append("0");
			}

			sb.append(i).append(PATH_POSTFIX);

			//final Path modelPath = Paths.get("/home/tgaengler/git/dmp-graph/dmp-graph/src/test/resources/dmpf_bsp1.nt");
			final Iterator<Statement> model = NonStrictNtParser.parse(sb.toString());

			LOG.debug("finished loading RDF model");

			parser.parse(model);

			// flush indices etc.
			handler.getHandler().closeTransaction();

			LOG.debug("finished writing " + handler.getHandler().getCountedStatements() + " RDF statements ('"
					+ handler.getHandler().getRelationshipsAdded() + "' added relationships) into graph db for data model URI '" + dataModelURI
					+ "'");
			NonStrictNtParser.close();
		}

		inserter.shutdown();

		LOG.debug("shutdown batch inserter");
	}
}
