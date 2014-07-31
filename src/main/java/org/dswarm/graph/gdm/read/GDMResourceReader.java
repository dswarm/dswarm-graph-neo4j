package org.dswarm.graph.gdm.read;

import org.dswarm.graph.json.Resource;

public interface GDMResourceReader {

	public Resource read();

	public long countStatements();
}
