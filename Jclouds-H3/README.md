# JClouds-H3 Storage Api Provider

Jclouds-H3 is a [Jclouds](https://github.com/apache/jclouds) API provider for [H3](https://github.com/CARV-ICS-FORTH/H3/) object store.
Provides a minimal but functional storage API for jclouds-compatible technologies(i.e. [s3proxy](https://github.com/gaul/s3proxy)). 

Implemented the Jclouds BlobStore interface, utilizing H3's Java wrapper(JH3),
similar to Amazon's S3, providing an efficient execution throughout the stack. 

## Installation
Detailed instructions on how to install, configure, and get the project running.
-To install locally:

**Note**: Firstly we need to have H3 and its wrappers installed locally:
- Instructions of how to install the shared libraries of H3lib, are [here](https://github.com/CARV-ICS-FORTH/H3/tree/master/h3lib) .
- Instructions of how to install the Java wrapper of H3, are [here](https://github.com/CARV-ICS-FORTH/H3/tree/master/JH3lib) .

**Note**: use JDK 9+ to avoid JH3's class missmatch.
##### Install Module with:
``` mvn install -Drat.skip=true -Dcheckstyle.skip=true ```

- need to skip RAT::check
- need to skip checkstyle::check

## Configuration
```properties
    jclouds.provider=h3
    jclouds.h3.basedir=file:///tmp/demo_h3via_jclouds
```

## Usage test

```java
// setup where the provider must store the files
Properties properties = new Properties();
properties.setProperty(H3Constants.PROPERTY_BASEDIR, "file:///tmp/demo_h3via_jclouds");
// setup the container name used by the provider (like bucket in S3)
String containerName = "testbucket";

// get a context with h3 that offers the portable BlobStore api
BlobStoreContext context = ContextBuilder.newBuilder("h3")
                .overrides(properties)
                .buildView(BlobStoreContext.class);

// create a container in the default location
BlobStore blobStore = context.getBlobStore();
blobStore.createContainerInLocation(null, containerName);

// add blob
BlobBuilder builder = blobStore.blobBuilder("test");
builder.payload("test data");
Blob blob = builder.build();
blobStore.putBlob(containerName, blob);

// retrieve blob
Blob blobRetrieved = blobStore.getBlob(containerName, "test");

// delete blob
blobStore.removeBlob(containerName, "test");

//close context
context.close();
```


## Known issues

- Unstable download when downloading parts with multiple connections per file (>100MB).
