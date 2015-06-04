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
package org.dswarm.graph.maintain.test;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.common.DMPStatics;
import org.dswarm.graph.gdm.test.BaseGDMResourceTest;
import org.dswarm.graph.test.Neo4jDBWrapper;

/**
 * @author tgaengler
 */
public abstract class MaintainResourceDeprecateTest extends BaseGDMResourceTest {

	private static final Logger LOG = LoggerFactory.getLogger(MaintainResourceDeleteTest.class);

	public MaintainResourceDeprecateTest(final Neo4jDBWrapper neo4jDBWrapper, final String dbTypeArg) {

		super(neo4jDBWrapper, dbTypeArg);
	}

	@Test
	public void testDeprecateDataModel() throws Exception {

		MaintainResourceDeprecateTest.LOG.debug("start deprecate data model test for maintain resource at {} DB", dbType);

		final String dataModelURI = "http://data.slub-dresden.de/resources/1";

		writeGDMToDBInternal(dataModelURI, BaseGDMResourceTest.DEFAULT_GDM_FILE_NAME);

		final String recordClassURI = "http://www.ddb.de/professionell/mabxml/mabxml-1.xsd#datensatzType";
		final int numberOfStatements = 191;

		readGDMFromDB(recordClassURI, dataModelURI, numberOfStatements, Optional.<Integer>absent());

		final String body = deprecateDataModel(dataModelURI);

		Assert.assertEquals("{\"deprecated\":152}", body);

		readGDMFromDB(recordClassURI, dataModelURI, 0, Optional.<Integer>absent());

		// try to deprecate data model again
		final String body2 = deprecateDataModel(dataModelURI);

		Assert.assertEquals("{\"deprecated\":0}", body2);

		// TODO: following thing doesn't work, since versioning is a bit broken atm
		//		// write same data again to data model
		//		writeGDMToDBInternal(dataModelURI, BaseGDMResourceTest.DEFAULT_GDM_FILE_NAME);
		//
		//		readGDMFromDB(recordClassURI, dataModelURI, numberOfStatements, Optional.<Integer>absent());

		MaintainResourceDeprecateTest.LOG.debug("finished deprecate data model test for maintain resource at {} DB", dbType);
	}

	public String deprecateDataModel(final String dataModelURI) throws JsonProcessingException {

		final ObjectNode requestJSON = objectMapper.createObjectNode();
		requestJSON.put(DMPStatics.DATA_MODEL_URI_IDENTIFIER, dataModelURI);
		final String request = objectMapper.writeValueAsString(requestJSON);

		final ClientResponse response = service().path("/maintain/deprecate/datamodel").type(MediaType.APPLICATION_JSON_TYPE).accept(
				MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, request);

		Assert.assertEquals("expected 200", 200, response.getStatus());

		final String body = response.getEntity(String.class);

		Assert.assertNotNull("response body shouldn't be null", body);

		return body;
	}
}
