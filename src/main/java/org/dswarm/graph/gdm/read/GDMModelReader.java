package org.dswarm.graph.gdm.read;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Model;

public interface GDMModelReader {

	public Model read() throws DMPGraphException;

	public long countStatements();
}
