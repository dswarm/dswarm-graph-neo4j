package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public interface GDMUpdateHandler extends CommonHandler {

	public void handleStatement(final String stmtUUID, final Resource resource, final long index, final long order) throws DMPGraphException;

	public void deprecateStatement(final long index);

	public Node deprecateStatement(final String uuid) throws DMPGraphException;
}
