package de.avgl.dmp.graph.maintain.test;

import com.sun.jersey.api.client.ClientResponse;
import de.avgl.dmp.graph.rdf.export.test.FullRDFExportOnEmbeddedDBTest;
import org.junit.Assert;
import org.junit.Test;

public class MaintainResourceDeleteOnEmbeddedDBTest extends FullRDFExportOnEmbeddedDBTest {

    public MaintainResourceDeleteOnEmbeddedDBTest() {
        super();
    }

    @Test
    public void testDelete() throws Exception {
        final String provenanceURI1 = "http://data.slub-dresden.de/resources/2";
        final String provenanceURI2 = "http://data.slub-dresden.de/resources/3";

        writeRDFToTestDBInternal(server, provenanceURI1);
        writeRDFToTestDBInternal(server, provenanceURI2);

        final ClientResponse response = service().path("/maintain/delete").delete(ClientResponse.class);

        System.out.println("response = " + response);

        Assert.assertEquals("expected 200", 200, response.getStatus());

        final String body = response.getEntity(String.class);

        Assert.assertNotNull("response body shouldn't be null", body);

        Assert.assertEquals("{\"deleted\":10404}", body);
    }
}
