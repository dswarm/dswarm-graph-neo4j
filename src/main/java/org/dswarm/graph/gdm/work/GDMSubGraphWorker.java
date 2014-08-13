package org.dswarm.graph.gdm.work;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Statement;

import java.util.Collection;
import java.util.Map;

public interface GDMSubGraphWorker {

	public Map<Long, Collection<Statement>> work() throws DMPGraphException;
}
