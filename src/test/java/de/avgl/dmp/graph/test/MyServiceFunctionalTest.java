package de.avgl.dmp.graph.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

import com.sun.jersey.api.client.Client;

public class MyServiceFunctionalTest {

	public static final Client				CLIENT			= Client.create();
	public static final String				MOUNT_POINT		= "/ext";
	private ObjectMapper					objectMapper	= new ObjectMapper();

	private static final RelationshipType	KNOWS			= DynamicRelationshipType.withName("KNOWS");

	@Test
	public void shouldReturnFriends() throws IOException {
		NeoServer server = CommunityServerBuilder.server().onPort(7499).withThirdPartyJaxRsPackage("de.avgl.dmp.graph", MOUNT_POINT).build();
		server.start();
		populateDb(server.getDatabase().getGraph());
		RestRequest restRequest = new RestRequest(server.baseUri().resolve(MOUNT_POINT), CLIENT);
		JaxRsResponse response = restRequest.get("service/friends/B");
		System.out.println(response.getEntity());
		List list = objectMapper.readValue(response.getEntity(), List.class);
		assertEquals(new HashSet<String>(Arrays.asList("A", "C")), new HashSet<String>(list));
		server.stop();
	}

	private void populateDb(GraphDatabaseService db) {
		Transaction tx = db.beginTx();
		try {
			Node personA = createPerson(db, "A");
			Node personB = createPerson(db, "B");
			Node personC = createPerson(db, "C");
			Node personD = createPerson(db, "D");
			personA.createRelationshipTo(personB, KNOWS);
			personB.createRelationshipTo(personC, KNOWS);
			personC.createRelationshipTo(personD, KNOWS);
			tx.success();
		} finally {
			tx.success();
			tx.close();
		}
	}

	private Node createPerson(GraphDatabaseService db, String name) {
		Index<Node> people = db.index().forNodes("people");
		Node node = db.createNode();
		node.setProperty("name", name);
		people.add(node, "name", name);
		return node;
	}

}
