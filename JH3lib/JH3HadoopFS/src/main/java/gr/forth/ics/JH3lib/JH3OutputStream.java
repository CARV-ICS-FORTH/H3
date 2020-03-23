package gr.forth.ics.JH3lib;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
class JH3OutputStream extends OutputStream
    implements StreamCapabilities {

  private static final Logger log = Logger.getLogger(JH3OutputStream.class);

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
  private JH3MultipartId multipartUploadId;

  /** ByteArray that keeps current block content */
  private ByteArrayOutputStream dataBlock;

  /** Index up to where data is written */
  private int position;

  JH3OutputStream(JH3 client, String bucket, String key, boolean overwrite, long blockSize) throws IOException {
    log.trace("JH3OutputStream - client: " + client + ", bucket: " + bucket + ", key: " 
      + key + ", blockSize: " + blockSize);

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
  public synchronized void flush() throws IOException {
    log.trace("JH3OutputStream:flush");
    // no-op
  }

  @Override
  public synchronized void write(int b) throws IOException {
    log.trace("JH3OutputStream:write - b: " + b);

    singleCharWrite[0] = (byte) b;
    write(singleCharWrite, 0, 1);
  }

  @Override
  public synchronized void write(byte[] source, int offset, int length) throws IOException {
    log.trace("JH3OutputStream:write - " + "source: " + Arrays.toString(source) + ", offset: " + offset
          + ", length: " + length);

    validateWriteArgs(source, offset, length);
    checkOpen();

    // Nothing to write
    if (length == 0)
      return;

    // Create a block if there is none
    createBlockIfNeeded();

    int written = writeToBlock(source, offset, length);
    int remaining = remainingBlockCapacity();
    if (written < length) {
      log.debug("writing more data than block has capacity; triggering upload");
      // not everything was written, block is full
      uploadCurrentBlock();

      // remaining bytes must be written to a new block
      this.write(source, offset + written, length - written);

    } else if (remaining == 0) {
      log.debug("the block is full after write; triggering upload");
      // the block is full after write, trigger an upload
      uploadCurrentBlock();
    }
  }

  @Override
  public void close() throws IOException {
    log.trace("JH3OutputStream:close");

    if (closed.getAndSet(true)) {
      log.debug("Ignoring close(), stream is already closed");
      // already closed
      return;
    }

    log.debug("JH3OutputStream: Closing block #" + blockCount);
    boolean hasBlock = hasActiveBlock();
    long bytes;

    if (multipartUploadId == null) {
      // No multipart upload has started, data fit in a single block
      if (hasBlock) {
        bytes = putObject();
        bytesSubmitted += bytes;
      }
    } else {
      // Upload last part and complete multipart
      if (hasBlock && (dataSize() > 0)) {
        uploadCurrentBlock();
        completeMultipartUpload();
      }
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    log.trace("JH3OutputStream:hasCapability - capability: " + capability);
    return false;
  }

  void checkOpen() throws IOException {
    log.trace("JH3OutputStream:checkOpen");

    if (closed.get()) {
      throw new IOException("JH3OutputStream is closed");
    }
  }

  private void validateWriteArgs(byte[] b, int offset, int length) {
    log.trace("JH3OutputStream:validateWriteArgs - b: " + Arrays.toString(b) + ", offset: " + offset
      + ", length: " + length);

    Preconditions.checkNotNull(b);

    if ((offset < 0) || (offset > b.length) || (length < 0) || ((offset + length) > b.length)
        || ((offset + length) < 0)) {
      throw new IndexOutOfBoundsException("write (b[" + b.length + "], " + offset + ", " + length + ')');
    }
  }

  private int putObject() throws IOException {
    log.trace("JH3OutputStream:putObject");

    try {

      int size = dataSize();
      JH3Object dataObj = new JH3Object(dataBlock.toByteArray(), size);
      client.createObject(bucket, key, dataObj);
      log.debug("Executing regular upload. size= " + size +", status= " + client.getStatus());
      clearBlock();
      return size;
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /* Multipart Operations */

  /**
   * Init multipart upload. Assumption: this is called from a synchronized block.
   * no-op if multipart has already started
   */
  private void initMultipartUpload() throws IOException {
    log.trace("JH3OutputStream:initMultipartUpload");

    // No need to init a multipart upload if one already exists
    if (multipartUploadId == null) {
      try {
        log.debug("Initiating Multipart upload. path: " + bucket + "/" + key);
        multipartUploadId = client.createMultipart(bucket, key);

        // if id is null, error has occurred
        if (multipartUploadId == null) {
          throw new JH3Exception("Cannot create a new multipart upload for: " + bucket + "/" + key);
        }
      } catch (JH3Exception e) {
        throw new IOException(e);
      }

    }
  }

  private void completeMultipartUpload() throws IOException {
    log.trace("JH3OutputStream:completeMultipartUpload");

    try {
      if (!client.completeMultipart(multipartUploadId))
        throw new JH3Exception("Cannot complete multipart upload for: " + bucket + "/" + key);
    } catch (JH3Exception e) {
      abortMultipartUpload();
      throw new IOException(e);
    }
  }

  private void abortMultipartUpload() throws IOException {
    log.trace("JH3OutputStream:abortMultipartUpload");

    try {
      if (!client.abortMultipart(multipartUploadId))
        throw new JH3Exception("Cannot abort multipart upload for: " + bucket + "/" + key);
    } catch (JH3Exception e) {
      throw new IOException(e);
    }
  }

  /*** Block operations ***/

  private synchronized void createBlockIfNeeded() {
    log.trace("JH3OutputStream:createBlockIfNeeded");

    if (dataBlock == null) {
      dataBlock = new ByteArrayOutputStream(blockSize);
      blockCount++;
    }
  }

  private synchronized void uploadCurrentBlock() throws IOException {
    log.trace("JH3OutputStream:uploadCurrentBlock");

    Preconditions.checkState(hasActiveBlock(), "No active block");
    log.debug("uploadCurrentBlock: Writing block #" + blockCount);
    initMultipartUpload();
    try {
      int size = dataSize();
      JH3Object dataObj = new JH3Object(dataBlock.toByteArray(), size);
      client.createPart(dataObj, multipartUploadId, blockCount);
      bytesSubmitted += size;
    } catch (JH3Exception e) {
      abortMultipartUpload();
      throw new IOException(e);
    } finally {
      clearBlock();
    }
  }

  private synchronized int writeToBlock(byte[] source, int offset, int length) {
    // Write only as much that can fit in block
    int written = Math.min(remainingBlockCapacity(), length);
    dataBlock.write(source, offset, written);
    return written;
  }

  private synchronized boolean hasActiveBlock() {
    return dataBlock != null;
  }

  private synchronized int dataSize() {
    return dataBlock.size();
  }

  private int remainingBlockCapacity() {
    return this.blockSize - dataBlock.size();
  }

  private void clearBlock() {
    log.trace("JH3OutputStream:clearBlock");

    synchronized (this) {
      log.debug("Clearing active block");
      dataBlock = null;
    }
  }

}
