package org.dswarm.graph.gdm.parse;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.versioning.VersionHandler;

/**
 * @author tgaengler
 */
public interface GDMUpdateHandler extends GDMHandler {

	public void handleStatement(final String stmtUUID, final Resource resource, final long index, final long order) throws DMPGraphException;

	public Node deprecateStatement(final String uuid) throws DMPGraphException;
}
