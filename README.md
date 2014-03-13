Sample Neo4j unmanaged extension for processing RDF
===================================================

This is an unmanaged extension. 

1. Build it: 

        mvn clean package

2. Copy the jar (from target/) to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=de.avgl.dmp.graph=/dmp

4. Start Neo4j server.

5. Query it over HTTP:

        curl http://localhost:7474/dmp/service/helloworld

6. The real service for write RDF (serialised as Turtle or N3) into the database is located at

        http://localhost:7474/dmp/rdf

   You can POST to this service a multipart/mixed object with the bytes of the RDF (Turtle or N3) and the second part should be a resource graph URI (as string)

7. You can check this resource via

        http://localhost:7474/dmp/rdf/ping 

