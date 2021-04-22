
package org.jclouds.h3.util;
/**
 * Utilities for the filesystem blobstore.
 */
public class Utils {
	/**
	 * Private constructor for utility class.
	 */
	private Utils() {
		// Do nothing
	}

	/**
	 * Determine if Java is running on a Mac OS
	 */
	public static boolean isMacOSX() {
		String osName = System.getProperty("os.name");
		return osName.contains("OS X");
	}


	/**
	 * Determine if Java is running on a windows OS
	 */
	public static boolean isWindows() {
		return System.getProperty("os.name", "").toLowerCase().contains("windows");
	}

}
