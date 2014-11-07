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
