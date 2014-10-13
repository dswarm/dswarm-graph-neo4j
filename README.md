An unmanaged extension for Neo4j for processing the GDM
=======================================================

This is an unmanaged extension for [Neo4j](http://www.neo4j.org/) for processing the [GDM](https://github.com/dswarm/dswarm-documentation/wiki/Graph-Data-Model) and RDF. 

1. Build it: 

        mvn clean package -DskipTests -PRELEASE
        
   (for release)

2. Copy the jar ("with dependencies"; from target/) to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line (like the following one) to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=org.dswarm.graph.resources=/graph
        
   (wherby, ````/graph```` is the relative (base) path of this unmanaged extension)

4. Start Neo4j server.

5. Query (check) it over HTTP:

        curl http://localhost:7474/graph/gdm/ping

6. The service for writing GDM (serialised as JSON) into the database is located at

        http://localhost:7474/graph/gdm/put

   You can POST to this service a ````multipart/mixed```` object with the bytes of the GDM and the second part should be a data model URI (as string), e.g.,
   
        curl -H "Content-Type:multipart/mixed" -F "content=@test-gdm.json; type=application/octet-stream" -F "content=http://data.example.com/resources/1" -X POST http://localhost:7474/graph/gdm/put -i -v
        
   (````test-gdm.json```` is the file name (relative path) of the GDM JSON and ````http://data.example.com/resources/1```` is the data model URI)

7. You can retrieve GDM from the database via

        http://localhost:7474/graph/gdm/get
 
   You can POST to this service a JSON object with key-value pairs for "record_class_uri" and "data_model_uri", e.g.,
   
        {
	        "record_class_uri": "http://example.com/myrecordclass",
	        "data_model_uri": "http://data.example.com/resources/1"
        }
   
   via
   
        curl -H "Content-Type:application/json" -H "Accept:application/json" --data-binary @test_request.json -X POST http://localhost:7474/graph/gdm/get -i -v

To compile the package to be able to run the JUnit tests, you need to run maven with the ````TEST```` profile.

Note: You can call

        curl -X DELETE http://localhost:7474/graph/maintain/delete

to trigger a clean-up remotely.

Note: the port of your neo4j stand-alone installation may vary from the standard port 7474. If you run your neo4j stand-alone installation on another port, you need to modify the unit tests re. this configuration.

See also: [documentation @ d:swarm wiki](https://github.com/dswarm/dswarm-documentation/wiki/Neo4j-extension)
