package gr.forth.ics.JH3lib;

import java.io.IOException;
import java.io.EOFException;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@InterfaceAudience.Private
@InterfaceStability.Evolving
public class H3InputStream extends FSInputStream implements CanSetReadahead {

	private final Logger LOG = LoggerFactory.getLogger(H3InputStream.class);
	private boolean ISDebug = true;

	/* Position that is set by seek() and returned by getPos() */
	private long pos;
	/* Closed bit. Volatile so reads are non-blocking. Updates must be in a
	 * synchronized block to guarantee an atomic check and set 
	 */
	private volatile boolean closed;
	private final JH3 client;
	private final String bucket;
	private final String key;
	private final String pathStr;
	private final long contentLength;
	private final String uri;
	/* Default readahead in S3A */
	private long readahead = 64 * 1024; 
	/* This is the actual position within the object, used by lazy seek
	 * to decide whether to seek on the next read or not.
	 */
	private long nextReadPos;
	/* The end of the content range of the last request. This is anbsolute 
	 * value of the range, not a length field.
	 */
	private long contentRangeFinish;
	/* The start of the content range of the last request. */
	private long contentRangeStart;


	/* Keep value of object in byte buffer */
	H3Object data;

	public H3InputStream(JH3 client, String bucket,
			String key, long contentLength, long readahead) throws IOException {

		if(ISDebug)
			System.out.println("H3InputSteam - " 
					+ "client: " + client + ", bucket: " + bucket 
					+ ", key: " + key + ", contentLength: " + contentLength 
					+ ", readahead: " + readahead);

		try {

			// Both bucket and key must be non empty
			Preconditions.checkArgument(!("".equals(bucket)), "Empty bucket");
			Preconditions.checkArgument(!("".equals(key)), "Empty key");
			this.bucket = bucket;
			this.key = key;
			this.pathStr = "tempValue";
			this.contentLength = contentLength;
			this.client = client;
			this.uri = "h3://" + this.bucket + "/" + this.key;
			setReadahead(readahead);
			// Read whole object
			data = client.readObject(bucket, key, 0L, contentLength);
		} catch (H3Exception e) {
			throw new IOException(e);
		}

		// Set position to beginning
		this.pos = 0;
	}



	@Override
	public synchronized void seek(long targetPos) throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:seek - targetPos = " + targetPos);

		checkNotClosed();

		// Do not allow negative seek
		if (targetPos < 0) {
			throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
		}

		if (this.contentLength <= 0){
			return;
		}

		// Lazy seek
		// nextReadPos = targetPos;
		pos = targetPos;
	}

	@Override
	public synchronized long getPos() throws IOException {
		if(ISDebug)
			System.out.println("H3InputStream:getPos");
		return pos;
		//return (nextReadPos < 0) ? 0 : nextReadPos;
	}


	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:seekToNewSource - "
					+ "targetPos: " + targetPos);
		return false;
	}

	@Override
	public synchronized int read() throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:read");

		checkNotClosed();

		if(this.contentLength == 0 || (getPos() >= contentLength))
			return -1;

		int b = data.getData()[(int)pos];
		pos++;
		return b;
	}

	@Override
	public synchronized int read(byte[] buffer, int offset, int length) throws IOException {


		if(ISDebug)
			System.out.println("H3InputStream:read - buffer = " + buffer + ", offset = "
					+ offset + ", length = " + length);

		checkNotClosed();
		//validatePositionedReadArgs(pos, buffer, offset, length);

		if (length == 0)
			return 0;

		else if (this.contentLength == 0 || (getPos() >= this.contentLength))
			//else if (getPos() > this.available())
			return -1;
		else{

			// Calculate how many bytes to read
			int remaining = this.available();
			int bytesToRead = Math.min(remaining, length);

			// Copy read bytes to given buffer
			System.arraycopy(data.getData(), (int) getPos(), buffer, offset, bytesToRead);

			// Move position depending on how many bytes were read
			pos += bytesToRead;
			if( ISDebug)
				System.out.println("bytesRead: " + bytesToRead);

			return bytesToRead;

		}
	}

	/**
	 * Operation which only seeks at the start of the series of operations; seeking back at the end
	 */
	@Override
	public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:readFully - "
					+ "position: " + position + ", buffer: " + buffer
					+ ", offset: " + offset + ", length: " + length);

		checkNotClosed();
		//validatePositionedReadArgs(position, buffer, offset, length);
		int nread = 0;
		synchronized(this){
			long oldPos = getPos();
			try{
				seek(position);
				while(nread < length){
					int nbytes = read(buffer, offset + nread, length - nread);
					if(nbytes < 0){
						throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
					}
					nread += nbytes;
				}
			} finally {
				seekQuietly(oldPos);
			}
		}
	}

	@Override
	public synchronized int available() throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:available");

		long remaining = remainingInFile();

		if (remaining > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;

		return (int) remaining;
	}

	@Override
	public synchronized void close() throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:close");

		if(!closed){
			closed = true;
			try{
				super.close();
			}finally{
				LOG.debug("Ignoring IOE on superclass close()", uri);
			}
		}
	}

	@Override
	public synchronized void setReadahead(Long readahead) {

		if(ISDebug)
			System.out.println("H3InputStream:setReadahead - "
					+ "readahead: " + readahead);

		if (readahead == null) {
			/* Default value */
			this.readahead = 64 * 1024;
		} else {
			Preconditions.checkArgument(readahead >= 0, "Negative readahead value");
			this.readahead = readahead;
		}
	}

	/* How many bytes are left to read */
	public synchronized long remainingInFile() {

		if(ISDebug)
			System.out.println("H3InputStream:remainingInFile: " + (this.contentLength - this.pos));

		return this.contentLength - this.pos;
	}

	public synchronized long getContentRangeStart(){

		if(ISDebug)
			System.out.println("H3InputStream:getContentRangeStart");

		return this.contentRangeStart;
	}

	public synchronized long getContentRangeFinish(){

		if(ISDebug)
			System.out.println("H3InputStream:getContentRangeFinish");

		return this.contentRangeFinish;
	}

	public synchronized long getReadahead(){

		if(ISDebug)
			System.out.println("H3InputStream:getReadahead");

		return readahead;
	}

	/**
	 * H3 Input stream does not support the mark and reset methods.
	 */
	@Override
	public boolean markSupported(){

		if(ISDebug)
			System.out.println("H3InputStream:markSupported");

		return false;
	}

	private void lazySeek(long targetPos, long len) throws IOException {

	}

	private void checkNotClosed() throws IOException {

		if(ISDebug)
			System.out.println("H3InputStream:checkNotClosed");

		if (closed){
			throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
		}
	}

	/* Seek without raising any exception. Used in finally clauses */
	private void seekQuietly(long positiveTargetPos) {
		try {
			seek(positiveTargetPos);
		}catch (IOException ioe){
			LOG.debug("Ignoring IOE on seek of {} to {}", uri, positiveTargetPos, ioe);
		}
	}
}



