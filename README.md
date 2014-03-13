Sample Neo4j unmanaged extension
================================

This is an unmanaged extension. 

1. Build it: 

        mvn clean package

2. Copy the jar (from target/) to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=de.avgl.dmp.graph=/dmp

4. Start Neo4j server.

5. Query it over HTTP:

        curl http://localhost:7474/dmp/service/helloworld

