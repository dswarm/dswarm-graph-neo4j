package org.dswarm.graph.xml.read;

import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.dswarm.graph.DMPGraphException;

/**
 * @author tgaengler
 */
public interface XMLReader {

	public XMLStreamWriter read(final OutputStream stream) throws DMPGraphException, XMLStreamException;
}
