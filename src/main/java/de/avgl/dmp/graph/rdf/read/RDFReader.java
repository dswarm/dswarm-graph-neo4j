package de.avgl.dmp.graph.rdf.read;

import com.hp.hpl.jena.rdf.model.Model;

public interface RDFReader {
	
	public Model read();
	
	public Model readAll();
	
	public long countStatements();
}
