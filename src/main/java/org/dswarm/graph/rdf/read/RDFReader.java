package org.dswarm.graph.rdf.read;

import com.hp.hpl.jena.rdf.model.Model;

import org.dswarm.graph.DMPGraphException;

public interface RDFReader {

	public Model read() throws DMPGraphException;

	public long countStatements();
}
