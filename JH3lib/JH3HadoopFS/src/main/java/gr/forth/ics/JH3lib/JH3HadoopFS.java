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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JH3HadoopFS extends FileSystem {
  /* (S3A) implements StreamCapabilities */

  public static final int DEFAULT_BLOCKSIZE = 32 * 1024 * 1024;
  public static final boolean DELETE_CONSIDERED_IDEMPOTENT = true;

  private Path workingDir;
  private URI uri;
  private String bucket;
  private JH3 client;
  private static final boolean H3_DEBUG = true;

  @Override
  public void initialize(URI name, Configuration originalConf) throws IOException {

    if (H3_DEBUG)
      System.out.println("JH3FS: initialize - URI: " + name + ", Configuration: " + originalConf);

    try {
      String h3Config = System.getenv("HADOOP_H3_CONFIG");
      if (h3Config == null)
        throw new IOException("HADOOP_H3_CONFIG environment variable is not set.");

      // TODO update with a valid user authentication method
      client = new JH3(JH3StoreType.JH3_STORE_CONFIG, h3Config, 0);
      setUri(name);
      workingDir = new Path("/").makeQualified(this.uri, this.getWorkingDirectory());
      bucket = name.getHost();
      super.initialize(name, originalConf);
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String getScheme() {

    if (H3_DEBUG)
      System.out.println("JH3FS: getScheme");

    return "h3";
  }

  @Override
  public URI getUri() {

    if (H3_DEBUG)
      System.out.println("JH3FS: getUri - URI= " + uri);

    return uri;

  }

  public FileStatus getFileStatus(final Path f) throws IOException {

    if (H3_DEBUG)
      System.out.println("JH3FS: getFileStatus - Path: " + f.toString());
    try {
      final Path path = qualify(f);
      String key = pathToKey(path);
      bucket = pathToBucket(path);
      // if key is empty, we are referring to a bucket instead of an object
      if (key.isEmpty()) {
        JH3BucketInfo bucketInfo = client.infoBucket(bucket);
        // If bucket exists, we consider it as a directory
        if (bucketInfo != null) {
          return new FileStatus(0, true, 0, 0, 0, path);
        } else {
          throw new FileNotFoundException("'" + path + "' does't exist in H3");
        }

      }

      // Retrieve list of object names that match the path
      ArrayList<String> objectNames = new ArrayList<>();
      do {
        ArrayList<String> retrieved = client.listObjects(bucket, key, objectNames.size());

        if(retrieved != null)
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
      System.out.println("objectNames = " + objectNames);
      //If its an exact match, the requested path is a file
      if (key.equals(closestMatch)) {
        // Retrieve stats of file
        JH3ObjectInfo objectInfo = client.infoObject(bucket, key);
        System.out.println("bucket = " + bucket);
        System.out.println("key = " + key);
        System.out.println("objectInfo = " + objectInfo);
        System.out.println(client.getStatus());
        return new FileStatus(objectInfo.getSize(), false, 0, 0, objectInfo.getLastModification().getSeconds(), path);
      } else {
        // Path emulates a directory
        return new FileStatus(0, true, 0, 0, 0, path);
      }
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  public boolean mkdirs(Path path, FsPermission permission)
          throws IOException {

    if (H3_DEBUG)
      System.out.println("JH3FS: mkdirs - Path: " + path + ", FsPermission: " + permission);

    final Path f = qualify(path);

    String key = pathToKey(f);
    String bucket = pathToBucket(f);

    FileStatus filestatus;
    try {
      filestatus = getFileStatus(f);

      if (filestatus.isDirectory()) {
        // Path is already a directory
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
            throw new FileAlreadyExistsException("Can't make directory since path '"
                    + fPart + "' is a file");
          }
        } catch (FileNotFoundException e) {
          // Sub-path doesnt exist, keep checking until root
          fPart = fPart.getParent();

          // We reached root and bucket doesn't exist; create it
          if (fPart == null) {
            try {
              System.out.println("Creating bucket: " + bucket + ". result: " + client.createBucket(bucket));
              //client.createBucket(bucket);
            } catch (JH3Exception ex) {
              throw new IOException(ex);
            }
          }
        }
      }
    }

    // At this point, the directory can be created
    // Path only contains bucket, create bucket instead of object
    if (key.isEmpty()) {
      //client.createBucket(bucket);
      return true;
    }

    // Add slash if key doesnt have one
    if (!key.endsWith("/")) {
      key = key + "/";
    }


    try {
      JH3Object dirObj = new JH3Object();
      System.out.println("bucket = " + bucket);
      System.out.println("key = " + key);
      System.out.println("dirObj = " + dirObj);
      System.out.println("buckets: " + client.listBuckets());
      boolean result = client.createObject(bucket, key, dirObj);
      System.out.println("create object result: " + result);
      System.out.println(client.getStatus());
      // Upload empty object to emulate directory
      return result; //client.createObject(bucket, key, dirObj);
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  public Path getWorkingDirectory() {

    if (H3_DEBUG)
      System.out.println("JH3FS: getWorkingDirectory");

    return workingDir;
  }

  public void setWorkingDirectory(Path newDir) {

    if (H3_DEBUG)
      System.out.println("JH3FS: setWorkingDirectory - Path: " + newDir);

    workingDir = newDir;
  }

  public FileStatus[] listStatus(Path f) throws IOException {

    if(H3_DEBUG)
      System.out.println("JH3FS: listStatus - Path: " + f);

    Path path = qualify(f);
    String bucket = pathToBucket(path);
    String prefix = pathToKey(path);
    try {
      // Special case, no bucket specified; list all buckets
      if (bucket == null) {
        ArrayList<String> buckets;
        buckets = client.listBuckets();
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
          String immediate_child = objectName.substring(prefix.length() + 1).split("/")[0];
          // an empty immediate child means that its the directory itself
          if (!immediate_child.isEmpty()) {
            children.add(immediate_child);
          }
        }

        // Get status for each immediate child
        FileStatus[] stats = new FileStatus[children.size()];
        int index = 0;
        for (String child : children) {
          stats[index++] = getFileStatus((createPath(bucket, prefix + "/" + child)));
        }

        return stats;
      } else {
        // Path isn't a directory
        FileStatus[] stats = new FileStatus[1];
        stats[0] = fileStatus;
        return stats;
      }
    } catch (JH3Exception e){
      throw new IOException(e);
    }
  }

  public boolean delete(Path f, boolean recursive) throws IOException {

    if(H3_DEBUG)
      System.out.println("JH3FS: delete - Path: " + f + ", recursive: " + recursive);

    Path path = qualify(f);
    String bucket = pathToBucket(path);
    String key = pathToKey(path);

    try{
      // Check is source is a file or directory
      FileStatus srcStatus = getFileStatus(path);

      // The following code sequence can assume that there is something at the end of the path,
      // otherwise FileNotFoundException was raised.

      if (srcStatus.isDirectory()) {
        FileStatus[] srcDirStatus = listStatus(path);

        // If listStatus returns an empty array, the directory is empty
        boolean isEmptyDir = srcDirStatus.length == 0;

        // Trying to delete root dir
        if (bucket == null) {
          return handleRootDelete(isEmptyDir, recursive);
        }

        // Trying to delete non empty directory with recursive == false
        if(!recursive && !isEmptyDir){
          throw new PathIsNotEmptyDirectoryException(f.toString());
        }

        // Add trailing slash to emulate a directory
        if(!key.endsWith("/")){
          key = key + "/";
        }
        // If directory (non-root) is empty, recursive doesnt matter; also it might actually
        // not exist
        if(isEmptyDir){

          client.deleteObject(bucket, key);
          return true;
        } else {
          // Directory (non-root) is not empty and recursive == true
          // Delete all descendants of specified path

          // Path refers to a bucket, not an object. Deleting a bucket also deletes all its
          // descendants so no iteration over files or subdirectories is needed
          if (key.equals("/")) {
            client.deleteBucket(bucket);
          } else {

            ArrayList<String> objectNames = new ArrayList<>();
            do {
              ArrayList<String> retrieved = client.listObjects(bucket, key, objectNames.size());
              objectNames.addAll(retrieved);
              // Keep listing objects until there are none left
            } while(client.getStatus() == JH3Status.JH3_CONTINUE);

            // Operation failed
            if(client.getStatus() != JH3Status.JH3_SUCCESS)
              throw new JH3Exception("listStatus: failed listing all objects of: " + bucket + " with prefix: " + key);

            // Delete everything under given directory
            for(String objectName: objectNames){
              client.deleteObject(bucket, objectName);
            }
          }
        }
      } else {
        // Path is a regular file, recursive doesn't matter
        if(H3_DEBUG)
          System.out.println("Deleting regular file object '" + path + "'");
        return client.deleteObject(bucket, key);
      }

      return true;
    } catch (FileNotFoundException | JH3Exception e){
      return false;
    }
  }


  /**
   *  Implementation doesn't follow hadoop specification.
   *  <pre>
   *      Fails if src is a directory and dst is a file.
   *      Fails if the parent of dst does not exist or is a file.
   *      Fails if dst is a directory that is not empty.
   *  </pre>
   * @param source path to be renamed
   * @param dest new path after rename
   * @return true if rename is successful
   * @throws IOException on failure
   */
  public boolean rename(Path source, Path dest) throws IOException {
    if (H3_DEBUG)
      System.out.println("JH3FS: rename - source: " + source + ", dest: " + dest);

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
            throw new IOException("Destination parent '" + parent.toString() +
                    "' is not a directory.\n" + "source = " + src.toString()
                    + "\ndestination = " + dst.toString());
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
          String filename = srcKey.substring(pathToKey(src.getParent()).length() + 1);
          String newDstKey = maybeAddTrailingSlash(dstKey) + filename;

          // No need to delete source since RenameObject automatically removes it
          client.moveObject(srcBucket, srcKey, newDstKey);
        } else {
          // src file ->  dst file (doesn't exist)
          client.moveObject(srcBucket, srcKey, dstKey);
        }

        if(parentStatus.length == 1){
          // Before rename, the file was the last element in the parent directory. Parent path
          // might not actually exist in H3, meaning that the parent directory might seems like its
          // deleted, so we need to actually create it.
          return mkdirs(parent, null);
        }
      } else {
        // src directory -> dst directory
        // Add trailing slash to keys since they emulate directories
        dstKey = maybeAddTrailingSlash(dstKey);
        srcKey = maybeAddTrailingSlash(srcKey);

        // Verify dest is not a descendant of source
        if (dstKey.startsWith(srcKey) && srcBucket.equals(dstBucket)) {
          throw new IOException("Cannot rename a directory to a subdirectory of itself");
        }

        // Calculate key size up to directory
        int srcSize = srcKey.length();
        // Retrieve status for each element under directory
        FileStatus[] srcDirStatus = listStatus(src);
        for (FileStatus dirStatus : srcDirStatus) {
          // Calculate destination
          String childKey = pathToKey(qualify(dirStatus.getPath()));
          String filename = childKey.substring(srcSize);
          String newDstKey = dstKey + filename;

          // Emulate directory if child is one
          if (dirStatus.isDirectory()) {
            newDstKey = newDstKey + '/';
          }

          boolean result = client.moveObject(srcBucket, childKey, newDstKey);

          // Failure in H3; used for debug
          if (H3_DEBUG && !result) {
            System.out.println("Rename failed");
            System.exit(-1);
          }
        }
        // Rename completed, remove source directory since it was not removed
        if (srcKey.isEmpty()) {
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

  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) {
    if(H3_DEBUG)
      System.out.println("JH3FS: append");

    throw new UnsupportedOperationException("Append is not supported by H3FileSystem");
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
                                   boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {

    if(H3_DEBUG)
      System.out.println("JH3FS: create - Path: " + f + ", permission: " + permission
              + ", overwrite: " + overwrite + ", blockSize: " + blockSize
              + ", Progressable: " + progress);

    try {
      final Path path = qualify(f);
      String key = pathToKey(path);
      bucket = pathToBucket(path);

      FileStatus status;
      try {
        status = getFileStatus(path);
        if(status.isDirectory()){
          throw new FileAlreadyExistsException(path + "it a directory");
        }

        if(!overwrite){
          throw new FileAlreadyExistsException(path + " already exists");
        }

      } catch (FileNotFoundException e){
        // Path doesn't exist
        // Check if any ancestor is a file
        Path parent = path.getParent();
        while(parent != null){
          try{
            status = getFileStatus(parent);

            // Closest ancestor is a directory, no need to check further, the path can be created
            if(status.isDirectory()) {
              break;
            }else{
              throw new IOException("Cannot create " + path + " since " + parent + " is a file");
            }
          } catch(FileNotFoundException e2){
            // Parent doesn't exist, check next ancestor
            parent = parent.getParent();
            // We reached root and bucket doesn't exist; create it
            if(parent == null){
              client.createBucket(bucket);
            }
          }
        }
      }

      // File can be created
      return new FSDataOutputStream(new JH3OutputStream(client, bucket, key,
              overwrite, blockSize), null);
    } catch (JH3Exception e){
      throw new IOException(e);
    }
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {

    if(H3_DEBUG)
      System.out.println("JH3FS: open - Path: " + f + ", bufferSize: " + bufferSize);

    final Path path = qualify(f);
    String key = pathToKey(path);
    bucket = pathToBucket(path);

    // Throws FileNotFoundException if file doesn't exist
    FileStatus status = getFileStatus(f);

    if (status.isDirectory())
      throw new FileNotFoundException("Can't open " + f
              + " because it is a directory");

    JH3InputStream s = new JH3InputStream(client, bucket, key, status.getLen(), 0);
    return new FSDataInputStream(s);
  }

  public void setUri(URI uri){
    this.uri = uri;
  }

  /** Turns a path into a H3 key.
   *
   */
  public String pathToKey(Path path){
    if(!path.isAbsolute()){
      path = new Path(workingDir, path);

    }

    if(path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()){
      return "";
    }

    return path.toUri().getPath().substring(1);
  }


  /** Turns a path into a H3 bucket.
   *
   */
  public String pathToBucket(Path path){
    if(!path.isAbsolute()){
      path = new Path(workingDir, path);
    }

    return path.toUri().getHost();
  }

  /** Create a qualified path from bucket and key.
   *
   */
  private Path createPath(String bucket, String key){
    return new Path(this.getScheme()+ "://" + bucket + "/" + key);
  }

  /**
   * Implements the logic from hadoop's delete specification.
   * Deleting root directories is never allowed.
   * @param isEmpty empty root flag
   * @param recursive recursive flag from command
   * @return a return code for the operation
   * @throws PathIOException if the operation was rejected
   */
  private boolean handleRootDelete(boolean isEmpty, boolean recursive) throws IOException {
    if(H3_DEBUG)
      System.out.println("JH3FS: handleRootDelete - isEmpty: " + isEmpty
              + ", recursive: " + recursive);

    // If root is empty, recursive flag doesn't matter
    if(isEmpty){
      return true;
    }

    // Root deletion is not permitted, return that nothing has changed to FS
    if(recursive){
      return false;
    } else {
      throw new PathIOException("Cannot delete root path");
    }


  }

  /**
   *  Add trailing slash if the path is not the root and does not already have one.
   * @param key key or ""
   * @return the key with a trailing slash or ""
   */
  private String maybeAddTrailingSlash(String key){
    if(!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    }

    return key;
  }

  /**
   * Qualify a path
   */
  public Path qualify(Path p) {

    //if(H3_DEBUG)
    //  System.out.println("JH3FS: qualify - path: " + p);

    return p.makeQualified(uri, workingDir);
    // String path = p.toString();

    // // Path is already qualified
    // if(path.startsWith(this.getScheme() + "://")) {
    //   return p;
    // }

    // // Remove leading slash from path if there is one
    // if(path.startsWith("/")){
    //   path = path.substring(1);
    // }

    // String wd = workingDir.toString();
    // // Working directory is root, no relative dirs
    // if(wd.equals("/")){
    //   System.out.println("no relative: " + this.getScheme() + "://" + path);
    //   if(path.isEmpty()) {
    //     return new Path(this.getScheme() + ":// ");
    //   }else{
    //     return new Path(this.getScheme() + "://" + path);
    //   }
    // }

    // // Remove leading and tailing slash from working directory, if there is one
    // if(wd.startsWith("/")){
    //   wd = wd.substring(1);
    // }

    // if(wd.endsWith("/")){
    //   wd = wd.substring(0, wd.length() -1);
    // }
    // // Return path with scheme and relative path
    // System.out.println("relative: " + this.getScheme() + "://" + wd + "/" + path);
    // return new Path(this.getScheme() + "://" + wd + "/" + path);

  }
}
