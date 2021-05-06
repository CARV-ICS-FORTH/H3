package gr.forth.ics.JH3lib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;

/**
 * The core H3 Filesystem implementation.
 * This implementation is based on Hadoop's FileSystem API specification, unless stated otherwise
 * in a method's documentation.
 * see <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/index.html">Hadoop API Specification</a>
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JH3HadoopFS extends FileSystem {
  /* (S3A) implements StreamCapabilities */

  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;
  public static final boolean DELETE_CONSIDERED_IDEMPOTENT = true;
  private static final String DIR_CHAR = "%";
  private static final Logger log = Logger.getLogger(JH3HadoopFS.class);
  private static final long readahead = 64 * 1024;   // default readAhead: 64KB
  private Path workingDir;
  private URI uri;
  private String bucket;
  private JH3 client;
  //private static final boolean H3_DEBUG = true;

  /**
   * Called after a new FileSystem instance is constructed.
   * Initializes a JH3 client. H3-specific configuration should be passed by HADOOP_H3_CONFIG environment variable.
   * @param name A URI whose authority section names the host, port, etc for this FileSystem
   * @param originalConf the configuration to use for the FS.
   * @throws IOException
   */
  @Override
  public void initialize(URI name, Configuration originalConf) throws IOException {
    log.trace("JH3FS: initialize - URI: " + name + ", Configuration: " + originalConf);

    try {
      String storageURI = System.getenv("H3LIB_STORAGE_URI");
      if (storageURI == null)
        throw new IOException("H3LIB_STORAGE_URI environment variable is not set.");

      // TODO update with a valid user authentication method
      client = new JH3(storageURI, 0);
      setUri(name);
      workingDir = new Path("/").makeQualified(this.uri, this.getWorkingDirectory());
      bucket = name.getHost();
      super.initialize(name, originalConf);
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Return the protocol scheme for the FileSystem.
   * @return "h3"
   */
  @Override
  public String getScheme() {
    log.trace("JH3FS: getScheme");
    return "h3";
  }

  /**
   * Returns a URI whose scheme and authority identify the FileSystem.
   * @return
   */
  @Override
  public URI getUri() {
    log.trace("JH3FS: getUri - URI= " + uri);
    return uri;

  }

  /**
   * Return a file status object that represents the path.
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws IOException on other problems
   * @throws FileNotFoundException when the path does not exist
   */
  public FileStatus getFileStatus(final Path f) throws IOException {
    log.trace("JH3FS: getFileStatus - Path: " + f.toString());
    
    try {
      final Path path = qualify(f);
      String key = pathToKey(path);
      bucket = pathToBucket(path);
      // if key is empty, we are referring to a bucket instead of an object
      if (key.isEmpty()) {
        JH3BucketInfo bucketInfo = client.infoBucket(bucket);
        // If bucket exists, we consider it as a directory
        if (bucketInfo != null) {
          log.debug(path + " emulates a directory. It is actually a bucket");
          return new FileStatus(0, true, 0, 0, 0, path);
        } else {
          throw new FileNotFoundException("'" + path + "' does't exist in H3");
        }

      }

      // Retrieve list of object names that match the path
      ArrayList<String> objectNames = new ArrayList<>();
      do {
        ArrayList<String> retrieved = client.listObjects(bucket, key, objectNames.size());
        if (retrieved != null)
          objectNames.addAll(retrieved);
        // Keep listing objects until there are none left
      } while (client.getStatus() == JH3Status.JH3_CONTINUE);

      // No match found in H3
      if (objectNames.size() == 0) {
        throw new FileNotFoundException("'" + path + "' doesn't exist in H3");
      }

      Collections.sort(objectNames);
      // Object names are sorted, so closest match is the first element
      String closestMatch = objectNames.get(0);
      log.debug("Listed objects from prefix: " + key + "= " + objectNames);
      // If its an exact match, the requested path is a file
      if (key.equals(closestMatch)) {
        // Retrieve stats of file
        JH3ObjectInfo objectInfo = client.infoObject(bucket, key);
        log.debug(path + " is a file. bucket= " + bucket + ", key= " + key + ", status= " + client.getStatus());
        log.debug("objectInfo= " + objectInfo);
        return new FileStatus(objectInfo.getSize(), false, 0, 0, objectInfo.getLastModification().getSeconds(), path);
      } else {
        // Path emulates a directory
        log.debug(path + " is a directory");
        return new FileStatus(0, true, 0, 0, 0, path);
      }
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Make the given path and all non-existent parents into directories.
   * Has the semantics of Unix {@code 'mkdir -p'}. Existence of the directory hierarchy is not an error.
   * @param path The path to create
   * @param permission the permissions to apply to the path
   * @return true if a directory was created or already existed
   * @throws FileAlreadyExistsException there is a file at the path specified
   * @throws IOException other IO problems
   */
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    log.trace("JH3FS: mkdirs - Path: " + path + ", FsPermission: " + permission);

    final Path f = qualify(path);

    String key = pathToKey(f);
    String bucket = pathToBucket(f);

    FileStatus filestatus;
    try {
      filestatus = getFileStatus(f);

      if (filestatus.isDirectory()) {
        // Path is already a directory
        log.debug(f + " is already a directory");
        return true;
      } else {
        // Path already exists but its a file
        throw new FileAlreadyExistsException("Path is a file: " + f);
      }
    } catch (FileNotFoundException fnfe) {
      // Walk path to root, ensuring closest ancestor is a directory, not a file
      Path fPart = f.getParent();
      while (fPart != null) {
        try {
          filestatus = getFileStatus(fPart);

          // New directory can be created
          if (filestatus.isDirectory()) {
            break;
          }

          // A sub-path in path is a file
          if (filestatus.isFile()) {
            throw new FileAlreadyExistsException("Can't make directory since path '" + fPart + "' is a file");
          }
        } catch (FileNotFoundException e) {
          // Sub-path doesnt exist, keep checking until root
          fPart = fPart.getParent();

          // We reached root and bucket doesn't exist; create it
          if (fPart == null) {
            try {
              client.createBucket(bucket);
              log.debug("Root bucket (" + bucket + ") doesn't exist, creating it. status=" + client.getStatus());
            } catch (JH3Exception ex) {
              throw new IOException(ex);
            }
          }
        }
      }
    }

    // At this point, the directory can be created
    try {
      // Path only contains bucket, create bucket instead of object
      if (key.isEmpty()) {
        log.debug(f + " only contains bucket: " + bucket + " without specifying key, creating it.");
        return client.createBucket(bucket);
      }

      // Add directory character if key doesnt have one
      key = maybeAddDirectoryCharacter(key);

      JH3Object dirObj = new JH3Object();
      // Upload empty object to emulate directory
      boolean result =client.createObject(bucket, key, dirObj);
      log.debug("Creating object to emulate a directory. (bucket= " + bucket + ", key= " + key +") status= " + client.getStatus());
      return result;
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Get the current working directory for the given file system.
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    log.trace("JH3FS: getWorkingDirectory");
    return workingDir;
  }

  /**
   * Set the current working directory for the given file system.
   * @param newDir The new working directory
   */
  public void setWorkingDirectory(Path newDir) {
    log.trace("JH3FS: setWorkingDirectory - Path: " + newDir);
    workingDir = newDir;
  }

  /**
   * List the statuses of the files/directories in the given path, if the path is a directory.
   * @param f The path to be listed
   * @return the statuses of the files/directories in the given path
   * @throws FileNotFoundException when the path does not exist
   */
  public FileStatus[] listStatus(Path f) throws IOException {
    log.trace("JH3FS: listStatus - Path: " + f);

    Path path = qualify(f);
    String bucket = pathToBucket(path);
    String prefix = pathToKey(path);
    try {
      // Special case, no bucket specified; list all buckets
      if (bucket == null) {
        ArrayList<String> buckets = client.listBuckets();
        log.debug("Listing all buckets for root path: " + buckets);
        FileStatus[] stats = new FileStatus[buckets.size()];
        for (int i = 0; i < buckets.size(); i++) {
          stats[i] = new FileStatus(0, true, 0, 0, 0, new Path("/" + buckets.get(i)));
        }
        return stats;
      }

      // Check if path is directory
      FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isDirectory()) {
        ArrayList<String> objectNames = new ArrayList<>();
        do {
          ArrayList<String> retrieved = client.listObjects(bucket, prefix, objectNames.size());
          objectNames.addAll(retrieved);
          // Keep listing objects until there are none left
        } while (client.getStatus() == JH3Status.JH3_CONTINUE);

        // Operation failed
        if (client.getStatus() != JH3Status.JH3_SUCCESS)
          throw new JH3Exception("listStatus: failed listing all objects of: " + bucket + " with prefix: " + prefix);

        Set<String> children = new HashSet<>();
        // Only immediate children should be listed
        for (String objectName : objectNames) {
          String immediate_child;
          // When prefix is empty we are listing a bucket
          if(prefix.isEmpty())
            immediate_child = objectName.split("/")[0];
          else{
            // prefix is part of an object key
            immediate_child = objectName.substring(prefix.length()+1).split("/")[0];
          }

          // an empty immediate child means that its the directory itself
          if (!immediate_child.isEmpty()) {
            children.add(immediate_child);
          }
        }

        // Get status for each immediate child
        FileStatus[] stats = new FileStatus[children.size()];
        int index = 0;

        log.debug(path + "is a directory. Listing all immediate children: " + children);
        for (String child : children) {
          stats[index++] = getFileStatus((createPath(bucket, prefix + "/" + child)));
        }

        return stats;
      } else {
        // Path isn't a directory
        log.debug(path + " is a file. Listing only itself");
        FileStatus[] stats = new FileStatus[1];
        stats[0] = fileStatus;
        return stats;
      }
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Delete a path. This operation is at least {@code O(files)}, with added
   * overheads to emulate the path. This operation is not atomic.
   * @param f The path to delete
   * @param recursive if path is a directory and set to true,
   *                  the directory is deleted, otherwise throws an exception.
   *                  In case of a file the recursive can be set to either true or false.
   * @return  true if the path existed and then was deleted; false if there wss not path in the first place, or the
   *          corner cases of root path deletion have surfaced.
   * @throws IOException when a directory or file could not be deleted
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    log.trace("JH3FS: delete - Path: " + f + ", recursive: " + recursive);

    Path path = qualify(f);
    String bucket = pathToBucket(path);
    String key = pathToKey(path);

    try {
      // Check is source is a file or directory
      FileStatus srcStatus = getFileStatus(path);

      // The following code sequence can assume that there is something at the end of
      // the path,
      // otherwise FileNotFoundException was raised.

      if (srcStatus.isDirectory()) {
        FileStatus[] srcDirStatus = listStatus(path);

        // If listStatus returns an empty array, the directory is empty
        boolean isEmptyDir = srcDirStatus.length == 0;

        // Trying to delete root dir
        if (bucket == null) {
          log.debug("Trying to delete root directory");
          return handleRootDelete(isEmptyDir, recursive);
        }

        // Trying to delete non empty directory with recursive == false
        if (!recursive && !isEmptyDir) {
          throw new PathIsNotEmptyDirectoryException(f.toString());
        }

        // If directory (non-root) is empty, recursive doesnt matter; also it might
        // actually
        // not exist
        if (isEmptyDir) {
          // Add trailing slash to emulate a directory
          key = maybeAddDirectoryCharacter(key);
          client.deleteObject(bucket, key);
          return true;
        } else {
          // Directory (non-root) is not empty and recursive == true
          // Delete all descendants of specified path

          // Path refers to a bucket, not an object. Deleting a bucket also deletes all
          // its descendants so no iteration over files or subdirectories is needed
          if (key.equals("/")) {
            // TODO check if key.equals or key.isempty is needed
            // TODO make sure this is still the case with updated client
            client.deleteBucket(bucket);
          } else {
            ArrayList<String> objectNames = new ArrayList<>();
            do {
              ArrayList<String> retrieved = client.listObjects(bucket, key, objectNames.size());
              log.debug("retrieved objects: " + retrieved);
              if(retrieved != null)
                objectNames.addAll(retrieved);
              // Keep listing objects until there are none left
            } while (client.getStatus() == JH3Status.JH3_CONTINUE);

            // Operation failed
            if (client.getStatus() != JH3Status.JH3_SUCCESS)
              throw new JH3Exception("failed listing all objects of: " + bucket + " with prefix: " + key);

            // Delete everything under given directory
            log.debug("Deleting everything under bucket: + " + bucket + ", key= " + key);
            for (String objectName : objectNames) {
              client.deleteObject(bucket, objectName);
              log.debug("Deleting key= " + objectName + ", status= " + client.getStatus());
            }
          }
        }
      } else {
        // Path is a regular file, recursive doesn't matter
        log.debug("Deleting regular file object '" + path + "'");
        return client.deleteObject(bucket, key);
      }

      return true;
    } catch (FileNotFoundException | JH3Exception e) {
      return false;
    }
  }

  /**
   * Implementation doesn't follow hadoop specification.
   * 
   * <pre>
   *      Fails if src is a directory and dst is a file.
   *      Fails if the parent of dst does not exist or is a file.
   *      Fails if dst is a directory that is not empty.
   * </pre>
   * 
   * @param source path to be renamed
   * @param dest   new path after rename
   * @return true if rename is successful
   * @throws IOException on failure
   */
  public boolean rename(Path source, Path dest) throws IOException {
      log.trace("JH3FS: rename - source: " + source + ", dest: " + dest);

    try {
      Path src = qualify(source);
      Path dst = qualify(dest);

      String srcKey = pathToKey(src);
      String dstKey = pathToKey(dst);
      String srcBucket = pathToBucket(src);
      String dstBucket = pathToBucket(dst);

      if (srcBucket == null)
        throw new IOException("Rename: source is root directory");

      if (dstBucket == null)
        throw new IOException("Rename: destination is root directory");

      log.debug("srcBucket= " + srcBucket);
      log.debug("dstBucket= " + dstBucket);
      if(!srcBucket.equals(dstBucket))
        throw new IOException("Rename: both source and destination must reside in the same bucket");

        // If there is no source file FileNotFoundException is raised
      FileStatus srcStatus = getFileStatus(src);

      if (src.equals(dst))
        throw new IOException("Rename: source and dest refer to the same file or directory");

      FileStatus dstStatus = null;
      try {
        dstStatus = getFileStatus(dst);

        // If destination doesn't exist, an exception is raised.
        // In this code sequence we assume that destination exists and we only need to
        // check if destination is valid for rename operation
        if (srcStatus.isDirectory()) {
          // Get status for each element in destination directory
          FileStatus[] dstDirStatus = listStatus(dst);
          if (dstStatus.isFile()) {
            // dir -> file
            throw new IOException("source is a directory and dest is a file");
          } else if (dstDirStatus.length > 0) {
            // dir -> non empty dir
            throw new IOException("destination is a non-empty directory");
          }
          // destination exists and is an empty directory
        } else {
          if (dstStatus.isFile()) {
            // file -> file
            throw new IOException("cannot rename onto existing file");
          }
        }

      } catch (FileNotFoundException e) {
        // Destination doesn't exist. Check if parent exists and is not a file
        Path parent = dst.getParent();
        try {
          FileStatus parentStatus = getFileStatus(parent);
          if (!parentStatus.isDirectory()) {
            throw new IOException("Destination parent '" + parent.toString() + "' is not a directory.\n" + "source = "
                + src.toString() + "\ndestination = " + dst.toString());
          }
        } catch (FileNotFoundException e2) {
          throw new IOException("Destination parent doesn't exist");
        }
      }

      // At this point, requested rename is a valid operation
      if (srcStatus.isFile()) {
        Path parent = src.getParent();
        FileStatus[] parentStatus = listStatus(src);

        if (dstStatus != null && dstStatus.isDirectory()) {
          // src file -> dst directory
          log.debug("Rename source is a file and destination is a directory. (src=" + src + ", dst=" + dst);
          String filename = srcKey.substring(pathToKey(src.getParent()).length() + 1);
          String newDstKey = maybeAddTrailingSlash(dstKey) + filename;
          client.moveObject(srcBucket, srcKey, newDstKey);
          log.debug("Status of rename: " + client.getStatus());
        } else {
          // src file -> dst file (doesn't exist)
          log.debug("Rename source and destination are files. (src=" + src + ", dst=" + dst);
          client.moveObject(srcBucket, srcKey, dstKey);
          log.debug("Status of rename: " + client.getStatus());
        }

        // TODO check if this is needed
        if (parentStatus.length == 1) {
          // Before rename, the file was the last element in the parent directory. Parent
          // path
          // might not actually exist in H3, meaning that the parent directory might seems
          // like its
          // deleted, so we need to actually create it.
          return mkdirs(parent, null);
        }
      } else {
        // src directory -> dst directory
        log.debug("Rename source and destination are directories. (src=" + src + ", dst=" + dst);

        // Verify dest is not a descendant of source
        if (dstKey.startsWith(srcKey) && srcBucket.equals(dstBucket)) {
          throw new IOException("Cannot rename a directory to a subdirectory of itself");
        }

        // Retrieve status for each element under directory
        FileStatus[] srcDirStatus = listStatus(src);

        // Calculate key size up to directory (including special character at the end)
        int srcSize = srcKey.length() + 1;
        // Add trailing slash to destination prefix key since it emulates a directory
        dstKey = maybeAddTrailingSlash(dstKey);
        for (FileStatus dirStatus : srcDirStatus) {
          // Calculate destination
          String childKey = pathToKey(qualify(dirStatus.getPath()));
          String filename = childKey.substring(srcSize);
          String newDstKey = dstKey + filename;

          // Emulate directory if child is one
          if (dirStatus.isDirectory()) {
            newDstKey = maybeAddDirectoryCharacter(newDstKey);
          }

          client.moveObject(srcBucket, childKey, newDstKey);
          log.debug("Status of rename: " + client.getStatus());
        }
        // Rename completed, remove source directory since it was not removed
        if (srcKey.isEmpty()) {
          // TODO check if deleting directories is needed
          // Directory which was renamed was a bucket
          client.deleteBucket(srcBucket);
          return true;
        }

        // If directory actually exists in H3, delete it
        // TODO maybe just remove if statement and keep only delete method
        if (client.infoObject(srcBucket, srcKey) != null) {
          client.deleteObject(srcBucket, srcKey);
        }
      }
      // Successful rename
      return true;
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Append to an existing file (this operation is not currently supported).
   * @param f the existing file to be appended
   * @param bufferSize the size of the buffer to be used
   * @param progress for reporting progress if it is not null
   * @throws IOException indicating that append is not supported
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) {
    log.trace("JH3FS: append");
    throw new UnsupportedOperationException("Append is not supported by H3FileSystem");
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * @param f the file name to open
   * @param permission the permission to set (not used by H3)
   * @param overwrite if a file with this name already exists, then if true, the file will be overwritten, and if
   *                  false an error will be thrown (not currently supported by H3)
   * @param bufferSize the size of the buffer to be used (not used by H3)
   * @param replication required block replication for the file (not used by H3)
   * @param blockSize the requested block size; is used to read the file by blockSize chunks
   * @param progress the progess reported (not used by H3)
   * @return a FSDataOutputStream object
   * @throws FileAlreadyExistsException if file already exists and overwrite is set to false
   * @throws IOException in the event of IO related errors
   */
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    log.trace("JH3FS: create - Path: " + f + ", permission: " + permission + ", overwrite: " 
      + overwrite + ", blockSize: " + blockSize + ", Progressable: " + progress);

    try {
      final Path path = qualify(f);
      String key = pathToKey(path);
      bucket = pathToBucket(path);

      FileStatus status;
      try {
        status = getFileStatus(path);
        if (status.isDirectory()) {
          throw new FileAlreadyExistsException(path + "it a directory");
        }

        if (!overwrite) {
          throw new FileAlreadyExistsException(path + " already exists");
        }

      } catch (FileNotFoundException e) {
        // Path doesn't exist
        // Check if any ancestor is a file
        Path parent = path.getParent();
        while (parent != null) {
          try {
            status = getFileStatus(parent);

            // Closest ancestor is a directory, no need to check further, the path can be
            // created
            if (status.isDirectory()) {
              break;
            } else {
              throw new IOException("Cannot create " + path + " since " + parent + " is a file");
            }
          } catch (FileNotFoundException e2) {
            // Parent doesn't exist, check next ancestor
            parent = parent.getParent();
            // We reached root and bucket doesn't exist; create it
            if (parent == null) {
              client.createBucket(bucket);
              log.debug("bucket doesn't exist for " + path + ", creating it. status= " + client.getStatus());
            }
          }
        }
      }

      // File can be created
      return new FSDataOutputStream(new JH3OutputStream(client, bucket, key, overwrite, blockSize), null);
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Opens a FSDataInputStream at the indicated path.
   * @param f The file name to open
   * @param bufferSize the size of the buffer to be used (not used by H3)
   * @return a FSDataInputStream object
   * @throws FileNotFoundException if file does not exist, or is a directory
   * @throws IOException in the event of IO related errors
   */
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      log.trace("JH3FS: open - Path: " + f + ", bufferSize: " + bufferSize);

    final Path path = qualify(f);
    String key = pathToKey(path);
    bucket = pathToBucket(path);

    // Throws FileNotFoundException if file doesn't exist
    FileStatus status = getFileStatus(f);

    if (status.isDirectory())
      throw new FileNotFoundException("Can't open " + f + " because it is a directory");

    JH3InputStream s = new JH3InputStream(client, bucket, key, status.getLen(), readahead);
    return new FSDataInputStream(s);
  }

  /**
   * Set the URI of the file system.
   * @param uri the file system URI
   */
  public void setUri(URI uri) {
    this.uri = uri;
  }

  /**
   * Turns a path into an H3 key.
   * @return the H3 Key
   */
  public String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);

    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  /**
   * Turns a path into a H3 bucket.
   * @return the H3 bucket
   */
  public String pathToBucket(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    return path.toUri().getHost();
  }

  /**
   * Create a qualified path from bucket and key.
   * @return  the qualified path
   */
  private Path createPath(String bucket, String key) {
    return new Path(this.getScheme() + "://" + bucket + "/" + key);
  }

  /**
   * Implements the logic from hadoop's delete specification. Deleting root
   * directories is never allowed.
   * 
   * @param isEmpty   empty root flag
   * @param recursive recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was rejected
   */
  private boolean handleRootDelete(boolean isEmpty, boolean recursive) throws IOException {
    log.trace("JH3FS: handleRootDelete - isEmpty: " + isEmpty + ", recursive: " + recursive);

    // If root is empty, recursive flag doesn't matter
    if (isEmpty) {
      return true;
    }

    // Root deletion is not permitted, return that nothing has changed to FS
    if (recursive) {
      return false;
    } else {
      throw new PathIOException("Cannot delete root path");
    }

  }

  /**
   * Add special character to indicate the key is a directory if the path is not
   * the root and does not already have one.
   * 
   * @param key key or ""
   * @return the key with a special character and the end or ""
   */
  private String maybeAddDirectoryCharacter(String key) {
    if (!key.isEmpty() && !key.endsWith(DIR_CHAR)) {
      return key + DIR_CHAR;
    }

    return key;
  }

  /**
   * Add trailing slash to the key. Used only for rename operations.
   * 
   * @param key ker or ""
   * @return the key with a trailing slash at the end or ""
   */
  private String maybeAddTrailingSlash(String key) {
    if (!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    }
    return key;
  }

  /**
   * Qualify a path
   */
  public Path qualify(Path p) {

    // if(H3_DEBUG)
    // System.out.println("JH3FS: qualify - path: " + p);

    return p.makeQualified(uri, workingDir);
    // String path = p.toString();

    // // Path is already qualified
    // if(path.startsWith(this.getScheme() + "://")) {
    // return p;
    // }

    // // Remove leading slash from path if there is one
    // if(path.startsWith("/")){
    // path = path.substring(1);
    // }

    // String wd = workingDir.toString();
    // // Working directory is root, no relative dirs
    // if(wd.equals("/")){
    // System.out.println("no relative: " + this.getScheme() + "://" + path);
    // if(path.isEmpty()) {
    // return new Path(this.getScheme() + ":// ");
    // }else{
    // return new Path(this.getScheme() + "://" + path);
    // }
    // }

    // // Remove leading and tailing slash from working directory, if there is one
    // if(wd.startsWith("/")){
    // wd = wd.substring(1);
    // }

    // if(wd.endsWith("/")){
    // wd = wd.substring(0, wd.length() -1);
    // }
    // // Return path with scheme and relative path
    // System.out.println("relative: " + this.getScheme() + "://" + wd + "/" +
    // path);
    // return new Path(this.getScheme() + "://" + wd + "/" + path);

  }
}
