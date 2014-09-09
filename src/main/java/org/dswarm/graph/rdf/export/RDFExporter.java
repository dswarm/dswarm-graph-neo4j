package org.dswarm.graph.rdf.export;

import com.hp.hpl.jena.query.Dataset;

import org.dswarm.graph.DMPGraphException;

/**
 *
 * @author tgaengler
 *
 */
public interface RDFExporter {

	public Dataset export() throws DMPGraphException;

	public long countStatements();

	public long processedStatements();

	public long successfullyProcessedStatements();
}
