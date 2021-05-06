package org.jclouds.h3.predicates.validators.internal;

import com.google.inject.Singleton;
import org.jclouds.h3.predicates.validators.H3BlobKeyValidator;

/**
 * Validates name for filesystem container blob keys implementation
 *
 * @see org.jclouds.rest.InputParamValidator
 * @see org.jclouds.predicates.Validator
 */
@Singleton
public class H3BlobKeyValidatorImpl extends H3BlobKeyValidator {

	@Override
	public void validate(String name) throws IllegalArgumentException {
		//blob key cannot be null or empty
		if (name == null || name.length() < 1)
			throw new IllegalArgumentException("Blob key can't be null or empty");

		//blobkey cannot start with / (or \ in Windows) character
		if (name.startsWith("\\") || name.startsWith("/"))
			throw new IllegalArgumentException("Blob key '" + name + "' cannot start with \\ or /");
	}

}
