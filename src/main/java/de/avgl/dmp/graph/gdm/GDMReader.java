package de.avgl.dmp.graph.gdm;

import de.avgl.dmp.graph.json.Model;

public interface GDMReader {
	
	public Model read();
	
	public long countStatements();
}
