package org.dswarm.graph.rdf.nx.parse;

import org.dswarm.graph.DMPGraphException;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class NxModelParser implements RDFParser {

	private static final Logger LOG = LoggerFactory.getLogger(RDFNeo4jHandler.class);

	private       RDFHandler rdfHandler;
	private final NxParser   model;

	public NxModelParser(final NxParser modelArg) {

		model = modelArg;
	}

	@Override
	public void setRDFHandler(final RDFHandler handler) {

		rdfHandler = handler;
	}

	@Override
	public void parse() throws DMPGraphException {

		while (model.hasNext()) {

			final Node[] st = model.next();

			if(st.length < 3) {

				final StringBuilder sb = new StringBuilder();

				int i = 0;

				for(final Node node : st) {

					sb.append(i).append(":'").append(node.toString()).append("' :: ");

					i++;
				}

				LOG.error("couldn't process statement, because it hasn't 3 parts (only '" + st.length + "'): " + sb.toString());

				continue;
			}

			rdfHandler.handleStatement(st);
		}
	}
}
