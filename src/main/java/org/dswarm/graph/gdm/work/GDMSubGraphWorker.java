package org.dswarm.graph.gdm.work;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Statement;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;

public interface GDMSubGraphWorker {

	public Map<String, Statement> work() throws DMPGraphException;
}
