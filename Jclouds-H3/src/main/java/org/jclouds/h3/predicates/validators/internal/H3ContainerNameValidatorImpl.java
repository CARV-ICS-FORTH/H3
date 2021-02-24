package org.jclouds.h3.predicates.validators.internal;


import com.google.inject.Singleton;
import org.jclouds.h3.predicates.validators.H3ContainerNameValidator;

/**
 * Validates container name for filesystem provider implementation
 *
 * @see org.jclouds.rest.InputParamValidator
 * @see org.jclouds.predicates.Validator
 */
@Singleton
public class H3ContainerNameValidatorImpl extends H3ContainerNameValidator {

	@Override
	public void validate(String name) throws IllegalArgumentException {
		//container name cannot be null or empty
		if (name == null || name.length() < 1)
			throw new IllegalArgumentException("Container name can't be null or empty");

		//container name cannot contains / (or \ in Windows) character
		if (name.contains("\\") || name.contains("/"))
			throw new IllegalArgumentException("Container name '" + name + "' cannot contain \\ or /");
	}

}
