package org.jclouds.h3.util.internal;

import com.google.inject.Inject;
import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.util.BlobUtils;
import org.jclouds.h3.strategy.internal.H3StorageStrategyImpl;

import javax.inject.Provider;

import static com.google.common.base.Preconditions.checkNotNull;

public class H3BlobUtilsImpl implements BlobUtils {

	protected final H3StorageStrategyImpl storageStrategy;
	protected final Provider<BlobBuilder> blobBuilders;

	@Inject
	public H3BlobUtilsImpl(LocalStorageStrategy storageStrategy, Provider<BlobBuilder> blobBuilders) {
		this.storageStrategy = (H3StorageStrategyImpl) checkNotNull(storageStrategy, "H3 Storage Strategy");
		this.blobBuilders = checkNotNull(blobBuilders, "H3  blobBuilders");
	}

	@Override
	public BlobBuilder blobBuilder() {
		return blobBuilders.get();
	}

	@Override
	public boolean directoryExists(String containerName, String directory) {
//		return storageStrategy.directoryExists(containerName, directory);
		System.out.println("[Jclouds-H3][directoryExists]Not yet implemented");

		return false;
	}

	@Override
	public void createDirectory(String containerName, String directory) {
//		storageStrategy.createDirectory(containerName, directory);
		System.out.println("[Jclouds-H3][createDirectory]Not yet implemented");

	}

	@Override
	public long countBlobs(String container, ListContainerOptions options) {
//		return storageStrategy.countBlobs(container, options);
		System.out.println("[Jclouds-H3][createDirectory]Not yet implemented");

		return 0;
	}

	@Override
	public void clearContainer(String container, ListContainerOptions options) {
		storageStrategy.clearContainer(container, options);
	}

	@Override
	public void deleteDirectory(String container, String directory) {
		System.out.println("[Jclouds-H3][deleteDirectory]Not yet implemented");

//		storageStrategy.deleteDirectory(container, directory);
	}
};
