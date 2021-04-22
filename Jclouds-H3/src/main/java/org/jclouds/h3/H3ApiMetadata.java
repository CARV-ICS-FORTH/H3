package org.jclouds.h3;

//import static org.jclouds.filesystem.reference.FilesystemConstants.PROPERTY_AUTO_DETECT_CONTENT_TYPE;
import java.net.URI;
import java.util.Properties;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.internal.BaseApiMetadata;
import org.jclouds.blobstore.BlobStoreContext;
import com.google.auto.service.AutoService;
import org.jclouds.h3.config.H3ContextModule;
import org.jclouds.rest.internal.BaseHttpApiMetadata;

/**
 * Implementation of {@link ApiMetadata} for jclouds H3
 */
@AutoService(ApiMetadata.class)
public class H3ApiMetadata extends BaseApiMetadata {

	@Override
	public Builder toBuilder() {
		return new Builder().fromApiMetadata(this);
	}

	public H3ApiMetadata() {
		super(new Builder());
	}

	protected H3ApiMetadata(Builder builder) {
		super(builder);
	}

	@Override
	public Properties getDefaultProperties() {
		Properties properties = BaseHttpApiMetadata.defaultProperties();
//		properties.setProperty(PROPERTY_AUTO_DETECT_CONTENT_TYPE, "false");
		return properties;
	}

	public static class Builder extends BaseApiMetadata.Builder<Builder> {

		protected Builder() {
			id("h3")
					.name("h3 api")
					.identityName("Unused")
					.defaultEndpoint("http://localhost/transient")
					.defaultIdentity(System.getProperty("user.name"))
					.defaultCredential("bar")
					.version("1")
					.documentation(URI.create("http://www.jclouds.org/documentation/userguide/blobstore-guide"))
					.defaultProperties(H3ApiMetadata.defaultProperties())
					.view(BlobStoreContext.class)
					.defaultModule(H3ContextModule.class);
		}

		@Override
		public H3ApiMetadata build() {
			return new H3ApiMetadata(this);
		}

		@Override
		protected Builder self() {
			return this;
		}
	}
}
