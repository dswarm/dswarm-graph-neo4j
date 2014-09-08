package org.dswarm.graph.gdm.work;

import java.util.Map;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Statement;

public interface GDMSubGraphWorker {

	public Map<String, Statement> work() throws DMPGraphException;
}
