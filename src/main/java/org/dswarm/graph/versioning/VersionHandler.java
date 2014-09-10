package org.dswarm.graph.versioning;

import org.dswarm.graph.DMPGraphException;
import org.dswarm.graph.json.Node;
import org.dswarm.graph.json.Resource;
import org.dswarm.graph.json.Statement;

/**
 * @author tgaengler
 */
public interface VersionHandler {

	public int getLatestVersion();

	public void setLatestVersion(final String dataModelURI) throws DMPGraphException;

	public void updateLatestVersion() throws DMPGraphException;

	public Range getRange();
}
