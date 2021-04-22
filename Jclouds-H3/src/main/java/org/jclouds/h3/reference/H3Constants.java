package org.jclouds.h3.reference;

/**
 * Common constants used in h3 provider
 */
public final class H3Constants {

	/** Specify the base directory where provider starts its file operations - must exists */
	public static final String PROPERTY_BASEDIR = "jclouds.h3.basedir";
	private H3Constants() {
		throw new AssertionError("intentionally unimplemented");
	}
}
