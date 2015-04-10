package org.dswarm.graph.index;

import java.io.File;

/**
 * @author tgaengler
 */
public final class Statics {

	public static final String INDEX_FILE_ENDING = ".dat";

	public static final String USER_DIR_IDENTIFIER = "user.dir";

	public static final String USER_DIR = System.getProperty(USER_DIR_IDENTIFIER);

	public static final String TARGET_IDENTIFIER = "target";

	public static final String DEFAULT_STORE_DIR = USER_DIR + File.separator + TARGET_IDENTIFIER;
}
