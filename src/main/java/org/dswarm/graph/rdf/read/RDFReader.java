package org.dswarm.graph.rdf.read;

import com.hp.hpl.jena.rdf.model.Model;

public interface RDFReader {

	public Model read();

	public long countStatements();
}
