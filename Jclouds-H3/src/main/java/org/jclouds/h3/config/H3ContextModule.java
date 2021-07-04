package org.jclouds.h3.config;

import com.google.inject.AbstractModule;
import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.LocalBlobRequestSigner;
//	import org.jclouds.blobstore.LocalStorageStrategy;
//	import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;
import org.jclouds.blobstore.util.BlobUtils;
import org.jclouds.h3.blobStore.H3BlobStore;
import org.jclouds.h3.predicates.validators.H3BlobKeyValidator;
import org.jclouds.h3.predicates.validators.H3ContainerNameValidator;
import org.jclouds.h3.predicates.validators.internal.H3BlobKeyValidatorImpl;
import org.jclouds.h3.predicates.validators.internal.H3ContainerNameValidatorImpl;
import org.jclouds.h3.strategy.internal.H3StorageStrategyImpl;
import org.jclouds.h3.util.internal.H3BlobUtilsImpl;

import static org.jclouds.h3.util.Utils.isWindows;

public class H3ContextModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(BlobStore.class).to(H3BlobStore.class);
		install(new BlobStoreObjectModule());
			if (isWindows()) {
				bind(ConsistencyModel.class).toInstance(ConsistencyModel.EVENTUAL);
			} else {
				bind(ConsistencyModel.class).toInstance(ConsistencyModel.STRICT);
			}
			bind(LocalStorageStrategy.class).to(H3StorageStrategyImpl.class);
			bind(BlobUtils.class).to(H3BlobUtilsImpl.class);
			bind(H3BlobKeyValidator.class).to(H3BlobKeyValidatorImpl.class);
			bind(H3ContainerNameValidator.class).to(H3ContainerNameValidatorImpl.class);
		bind(BlobRequestSigner.class).to(LocalBlobRequestSigner.class);
	}


}
