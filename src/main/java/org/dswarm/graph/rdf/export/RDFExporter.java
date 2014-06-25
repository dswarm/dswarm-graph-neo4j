package org.dswarm.graph.rdf.export;

import com.hp.hpl.jena.query.Dataset;

/**
 *
 * @author tgaengler
 *
 */
public interface RDFExporter {

	public Dataset export();

	public long countStatements();

	public long processedStatements();

	public long successfullyProcessedStatements();
}
