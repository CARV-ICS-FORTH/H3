package gr.forth.ics.JH3lib;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StreamCapabilities;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
class H3OutputStream extends OutputStream 
  implements StreamCapabilities {

  private static final Logger LOG =
    LoggerFactory.getLogger(H3OutputStream.class);

  private boolean OSDebug = false;
  private final JH3 client;

  /** Bucket to which object is uploaded */
  private final String bucket;

  /** Object being uploaded */
  private final String key;

  /** Overwrite if exists */
  private boolean overwrite;

  /** Size of all blocks */
  private final int blockSize;

  /** Total bytes for uploads submitted so far */
  private long bytesSubmitted;

  /** Preallocate byte buffer for writing single characters */
  private final byte[] singleCharWrite = new byte[1];

  /** Closed flag */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Count of blocks uploaded */
  private int blockCount = 0;

  /** Id of multipart upload */
  private H3MultipartId multipartUploadId;

  /** ByteArray that keeps current block content */
  private ByteArrayOutputStream dataBlock;

  /** Index up to where data is written */
  private int position;


  H3OutputStream(JH3 client, String bucket, String key, boolean overwrite, long blockSize)
      throws IOException {

      if(OSDebug)
        System.out.println("H3OutputStream - client: " + client
            + ", bucket: " + bucket + ", key: " + key 
            + ", blockSize: " + blockSize);

      this.client = client;
      this.bucket = bucket;
      this.key = key;
      this.overwrite = overwrite;
      this.blockSize = (int) blockSize;
      this.bytesSubmitted = 0;
      // create the first block. This guarantees that an open + close sequence
      // writes a 0-byte entry.
      createBlockIfNeeded();
  }

  @Override
  public synchronized void flush() throws IOException{
    // no-op 
    if(OSDebug)
      System.out.println("H3OutputStream:flush");
  }

  @Override
  public synchronized void write(int b) throws IOException{
    if(OSDebug)
      System.out.println("H3OutputStream:write - b: " + b);

    singleCharWrite[0] = (byte) b;
    write(singleCharWrite, 0, 1);
  }

  @Override
  public synchronized void write(byte[] source, int offset, int length)
    throws IOException {

    if(OSDebug)
      System.out.println("H3OutputStream:write - "
          + "source: " + Arrays.toString(source)
          + ", offset: " + offset
          + ", length: " + length);

    validateWriteArgs(source, offset, length);
    checkOpen();

    // Nothing to write
    if(length == 0)
      return;

    // Create a block if there is none 
    createBlockIfNeeded();

    int written = writeToBlock(source, offset, length);
    int remaining = remainingBlockCapacity();
    if(written < length) {
      LOG.debug("writing more data than block has capacity -triggering upload");
      // not everything was written, block is full
      uploadCurrentBlock();

      // remaining bytes must be written to a new block
      this.write(source, offset + written, length - written);

    }else if (remaining == 0){
      // the block is full after write, trigger an upload
      uploadCurrentBlock();
    }
  }

  @Override
  public void close() throws IOException {
    if(OSDebug)
      System.out.println("H3OutputStream:close");

    if(closed.getAndSet(true)) {
      // already closed
      LOG.debug("Ignoring close(), stream is already closed");
      return;
    }

    LOG.debug("{}: Closing block #{}", this, blockCount);
    boolean hasBlock = hasActiveBlock();
    long bytes;

    if(multipartUploadId == null) {
      // No multipart upload has started, data fit in a single block
      if (hasBlock) {
        bytes = putObject(); 
        bytesSubmitted += bytes;
      }
    } else {
      // Upload last part and complete multipart 
      if(hasBlock && (dataSize() > 0)) {
        uploadCurrentBlock();
        completeMultipartUpload();
      }
    }
  }
  @Override
  public boolean hasCapability(String capability){
    if(OSDebug)
      System.out.println("H3OutputStream:hasCapability - capability: " 
          + capability);

    return false;
  }

  void checkOpen() throws IOException {
    if(OSDebug)
      System.out.println("H3OutputStream:checkOpen");

    if(closed.get()){
      throw new IOException("H3OutputStream is closed");
    }
  }

  private void validateWriteArgs(byte[] b, int offset, int length) {

      if(OSDebug)
        System.out.println("H3OutputStream:validateWriteArgs - b: " + Arrays.toString(b)
            + ", offset: " + offset + ", length: " + length);

      Preconditions.checkNotNull(b);

      if(( offset < 0) || (offset > b.length) || ( length < 0) ||
          ((offset + length) > b.length) || ((offset + length) < 0)) {
        throw new IndexOutOfBoundsException(
            "write (b[" + b.length + "], " + offset + ", " +
            length + ')');
          }
  }

  private int putObject() throws IOException {
    if(OSDebug)
      System.out.println("H3OutputStream:putObject");

    LOG.debug("Executing regular upload");
    try {
      int size = dataSize();
      H3Object dataObj = new H3Object(dataBlock.toByteArray(), size);
      client.createObject(bucket, key, dataObj);
      clearBlock();
      return size;
    }catch (H3Exception e){
      throw new IOException(e);
    }
  }

  /* Multipart Operations */

  /** Init multipart upload. Assumption: this is called from a 
   * synchronized block. 
   * no-op if multipart has already started 
   * */
  private void initMultipartUpload() throws IOException{

    if(OSDebug)
      System.out.println("H3OutputStream:initMultipartUpload");

    // No need to init a multipart upload if one already exists
    if(multipartUploadId == null) {
      LOG.debug("Initiating Multipart upload {}/{}", bucket, key);
      try {
        multipartUploadId = client.createMultipart(bucket, key);

        // if id is null, error has occurred
        if (multipartUploadId == null) {
          throw new H3Exception("Cannot create a new multipart upload for: "
                  + bucket + "/" + key);
        }
      } catch (H3Exception e) {
        throw new IOException(e);
      }

    }
  }

  private void completeMultipartUpload() throws IOException {

    if(OSDebug)
      System.out.println("H3OutputStream:completeMultipartUpload");

    try {
      if(!client.completeMultipart(multipartUploadId))
        throw new H3Exception("Cannot complete multipart upload for: "
            + bucket + "/" + key);
    } catch (H3Exception e) {
      abortMultipartUpload();
      throw new IOException(e);
    }
  }

  private void abortMultipartUpload() throws IOException {

    if(OSDebug)
      System.out.println("H3OutputStream:abortMultipartUpload");

    try {
      if(!client.abortMultipart(multipartUploadId))
        throw new H3Exception("Cannot abort multipart upload for: "
            + bucket + "/" + key);
    } catch (H3Exception e) {
      throw new IOException(e);
    }
  }

  /*** Block operations ***/

  private synchronized void createBlockIfNeeded() {

    if(OSDebug)
      System.out.println("H3OutputStream:createBlockIfNeeded");

    if(dataBlock == null) {
      dataBlock = new ByteArrayOutputStream(blockSize);
      blockCount++;
    }
  }


  private synchronized void uploadCurrentBlock() throws IOException {

    if(OSDebug)
      System.out.println("H3OutputStream:uploadCurrentBlock");

    Preconditions.checkState(hasActiveBlock(), "No active block");
    LOG.debug("Writing block # {}", blockCount);

    initMultipartUpload();
    try {
      int size = dataSize();
      H3Object dataObj = new H3Object(dataBlock.toByteArray(), size);
      client.createPart(dataObj, multipartUploadId, blockCount);
      bytesSubmitted += size;
    }catch(H3Exception e){
      abortMultipartUpload();
      throw new IOException(e);
    }finally{
      clearBlock();
    }
  }

  private synchronized int writeToBlock(byte[] source, int offset,
      int length) {

    // Write only as much that can fit in block
    int written = Math.min(remainingBlockCapacity(), length);
    dataBlock.write(source, offset, written);
    return written;
  }

  private synchronized boolean hasActiveBlock() {

    if(OSDebug)
      System.out.println("H3OutputStream:hasActiveBlock");

    return dataBlock != null;
  }


  private synchronized int dataSize(){

    if(OSDebug)
      System.out.println("H3OutputStream:dataSize");

    return dataBlock.size();
  }

  private int remainingBlockCapacity(){

    if(OSDebug)
      System.out.println("H3OutputStream:remainingBlockCapacity");

    return this.blockSize - dataBlock.size();
  }

  private void clearBlock(){

    if(OSDebug)
      System.out.println("H3OutputStream:clearBlock");

    if(dataBlock != null) {
      LOG.debug("Clearing active block");
    }

    synchronized(this){
      dataBlock = null;
    }
  }

}
