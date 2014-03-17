Sample Neo4j unmanaged extension for processing RDF
===================================================

This is an unmanaged extension. 

1. Build it: 

        mvn clean package

2. Copy the jar (from target/) to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=de.avgl.dmp.graph.resources=/dmp

4. Start Neo4j server.

5. Query it over HTTP:

        curl http://localhost:7474/dmp/service/helloworld

6. The real service for write RDF (serialised as Turtle or N3) into the database is located at

        http://localhost:7474/dmp/rdf/put

   You can POST to this service a multipart/mixed object with the bytes of the RDF (Turtle or N3) and the second part should be a resource graph URI (as string)

7. You can check this resource via

        http://localhost:7474/dmp/rdf/ping

8. You can retrieve RDF from the database via

        http://localhost:7474/dmp/rdf/get
 
   You can POST to this service a JSON object with key-value pairs for "record_class_uri" and "resource_graph_uri"

To compile the package for the neo4j stand-alone, you need to run maven with the RELEASE profile, e.g.,

       mvn clean package -DskipTests -PRELEASE

To compile the package to be able to run the JUnit tests, you need to run mavhen with the TEST profile.

You can check your deployed unmanaged extension via the following unit tests: 

 - RDFResourceTest#testPing()
 - RDFResourceTest#writeRDFToRunningDB()
 - RDFResourceTest#readRDFFromRunningDB()

Note: the database wouldn't get cleaned after the unit tests were executed, so you need to take care of cleaning up the database to ensure test correctness. 

