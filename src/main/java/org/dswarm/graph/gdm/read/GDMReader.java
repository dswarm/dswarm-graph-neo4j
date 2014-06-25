package org.dswarm.graph.gdm.read;

import org.dswarm.graph.json.Model;

public interface GDMReader {

	public Model read();

	public long countStatements();
}
