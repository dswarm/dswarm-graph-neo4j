package org.dswarm.graph.delta;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

/**
 * Holds references for static fields.
 * 
 * @author tgaengler
 */
public interface DMPStatics {

	/**
	 * The delimiter of an attribute path.
	 */
	public static final Character	ATTRIBUTE_DELIMITER	= '\u001E';

	public static final Label LEAF_LABEL = DynamicLabel.label("__LEAF__");
}
