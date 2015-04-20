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
package org.dswarm.graph.batch.rdf.pnx.parse.test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.io.Resources;
import de.knutwalker.ntparser.NonStrictNtParser;
import de.knutwalker.ntparser.model.NtModelFactory;
import de.knutwalker.ntparser.model.Statement;
import org.junit.Test;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.graph.batch.rdf.pnx.DataModelRDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.RDFNeo4jProcessor;
import org.dswarm.graph.batch.rdf.pnx.parse.DataModelRDFNeo4jHandler;
import org.dswarm.graph.batch.rdf.pnx.parse.PNXParser;
import org.dswarm.graph.batch.rdf.pnx.parse.RDFHandler;
import org.dswarm.graph.batch.rdf.pnx.parse.RDFParser;

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
		final BatchInserter inserter = BatchInserters.inserter("target/test_data", config);

		final RDFNeo4jProcessor processor = new DataModelRDFNeo4jProcessor(inserter, dataModelURI);
		final RDFHandler handler = new DataModelRDFNeo4jHandler(processor);
		final RDFParser parser = new PNXParser(handler);

		LOG.debug("finished initializing batch inserter");

		LOG.debug("start batch import");

		final URL fileURL = Resources.getResource("dmpf_bsp1.nt");
		final byte[] file = Resources.toByteArray(fileURL);
		final InputStream stream = new ByteArrayInputStream(file);
		final Iterator<Statement> model = NonStrictNtParser.parse(stream, NtModelFactory.INSTANCE());

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
