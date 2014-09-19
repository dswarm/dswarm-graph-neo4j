package org.dswarm.graph.versioning;

import org.dswarm.graph.DMPGraphException;

import com.google.common.base.Optional;

/**
 * @author tgaengler
 */
public interface VersionHandler {

	public int getLatestVersion();

	public void setLatestVersion(final Optional<String> optionalDataModelURI) throws DMPGraphException;

	public void updateLatestVersion() throws DMPGraphException;

	public Range getRange();
}
