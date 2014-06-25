package org.dswarm.graph;


/**
 * The exception class for DMP graph exceptions.<br>
 *
 */

public class DMPGraphException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new DMP graph exception with the given exception message.
	 *
	 * @param exception the exception message
	 */
	public DMPGraphException(final String exception) {

		super(exception);
	}

	/**
	 * Creates a new DMP exception with the given exception message
	 * and a cause.
	 *
	 * @param message the exception message
	 * @param cause   a previously thrown exception, causing this one
	 */
	public DMPGraphException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
