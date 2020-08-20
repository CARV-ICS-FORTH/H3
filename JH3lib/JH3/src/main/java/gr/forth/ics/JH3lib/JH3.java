package gr.forth.ics.JH3lib;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;

import java.io.Serializable;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.*;
import java.util.logging.Logger;

/**
 * A Java client for H3.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3 implements Serializable{

    private static final Logger log = Logger.getLogger(JH3.class.getName());
    private static final Cleaner cleaner = Cleaner.create();
    private Pointer handle;
    private NativeAuth token;
    private JH3Status status = JH3Status.JH3_SUCCESS;

    /** The maximum bucket name size. */
    public static int H3_BUCKET_NAME_SIZE = JH3Interface.H3_BUCKET_NAME_SIZE;
    /** The maximum object name size. */
    public static int H3_OBJECT_NAME_SIZE = JH3Interface.H3_OBJECT_NAME_SIZE;

    /**
     * If empty constructor is called, user must call init
     */
    //public JH3Client(){ }

     //// TODO Consider if this is needed
    //public void free() {
    //    JH3libInterface.INSTANCE.H3_Free(handle);
    //    handle = null;      // handle is no longer valid
    //}
    // }

    /**
     * Create a JH3 client.
     * Example backend URIs include (defaults for each type shown):
     * ``file:///tmp/h3`` for local filesystem
     * ``kreon://127.0.0.1:2181`` for Kreon, where the network location refers to the ZooKeeper host and port
     * ``rocksdb:///tmp/h3/rocksdb`` for `RocksDB <https://rocksdb.org>`_
     * ``redis://127.0.0.1:6379`` for `Redis <https://redis.io>`_

     * @param storageURI  The backend storage URI.
     * @param userId      The user that performs all operations.
     */
    public JH3(String storageURI, int userId) throws JH3Exception {
        Pointer uri = new Memory(storageURI.length() + 1);
        uri.setString(0, storageURI);
        handle = JH3Interface.INSTANCE.H3_Init(uri);
        if(handle == null){
            throw new JH3Exception("Could not initialize H3");
        }
        token = new NativeAuth(userId);

        // When object is garbage collected, H3_Free is called to free the handle
        cleaner.register(this, () -> {JH3Interface.INSTANCE.H3_Free(handle);});
    }

    /**
     * Create a JH3 client.
     * @param storageURI  The backend storage URI.
     * @param token       The authentication token of the user that performs all operations.
     */
    public JH3(String storageURI, JH3Auth token) throws JH3Exception {
        this(storageURI, token.getUserId());
    }

    /**
     * Get the current version of the JH3 client.
     * @return the version string.
     */
    public String getVersion(){ return JH3Interface.INSTANCE.H3_Version().getString(0); }

    /**
     * Get the status of the last operation performed.
     * @return The status of the last operation.
     */
    public JH3Status getStatus() { return status; }

    /**
     * Get the name of the status of the last operation performed.
     * @return The name of the status of the last operation.
     */
    public String getStatusName(){ return status.name(); }

    /**
     * Get the authentication token of the user performing the operations.
     * @return The authentication token.
     */
    public JH3Auth getToken(){
        return new JH3Auth(token.userId);
    }

    /* Bucket Management methods */

    /**
     * Create a bucket. The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_EXISTS} - The bucket already exists.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_STORE_ERROR} - A storage provider error
     *
     * @param bucketName The bucket name.
     * @return  <code>true</code>if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public boolean createBucket(String bucketName) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        bucket.setString(0, bucketName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CreateBucket(handle, token, bucket));
        return operationSucceeded(status);
    }

    /**
     * Delete a bucket. The bucket must be empty and the token must grant access to it in order to be deleted. The
     * status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket does not exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - A storage provider error or the user has no access rights, or the bucket is
     * not empty.
     *
     * @param bucketName The bucket name.
     * @return  <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public boolean deleteBucket(String bucketName) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        bucket.setString(0, bucketName);

        status = JH3Status.fromInt((JH3Interface.INSTANCE.H3_DeleteBucket(handle, token, bucket)));
        return operationSucceeded(status);
    }

    /**
     * List buckets associated with a user. The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - A storage provider error
     * @return The list of buckets if the operation was successful, <code>null</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public ArrayList<String> listBuckets() throws JH3Exception {
        PointerByReference bucketNameArray = new PointerByReference();
        IntBuffer nBuckets = IntBuffer.allocate(1);

        // Retrieve bucket names
        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_ListBuckets(handle, token, bucketNameArray, nBuckets));

        // Return empty list on failure
        if(!operationSucceeded(status))
            return null;


        // Operation was successful
        ArrayList<String> buckets = new ArrayList<>();
        for(int i = 0, arrayOffset  = 0; i < nBuckets.get(0); i++) {
            String tmp = bucketNameArray.getValue().getString(arrayOffset);
            buckets.add(tmp);
            arrayOffset += tmp.length();
            while(bucketNameArray.getValue().getByte(arrayOffset) == '\0')
                arrayOffset++;
        }

        // Free internal buffer (only allocated when operation was successful)
        Native.free(Pointer.nativeValue(bucketNameArray.getValue()));

        return buckets;
    }

    /**
     * Retrieve information about a bucket. The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - A storage provider error
     *
     * @param bucketName The bucket name.
     * @param getStats If <code>true</code>, aggregate object information will also be produced at the cost of response
     *                 time.
     * @return  The bucket information if the operation was successful, <code>null</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public JH3BucketInfo infoBucket(String bucketName, boolean getStats) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        NativeBucketInfo bucketInfo= new NativeBucketInfo();
        byte retrieveStats = (byte) (getStats? 1:0);        // map boolean to byte

        bucket.setString(0, bucketName);
        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_InfoBucket(handle, token, bucket, bucketInfo, retrieveStats));
        if (!operationSucceeded(status))
            return null;

        if(getStats){
            JH3BucketStats stats = new JH3BucketStats(bucketInfo.stats.size.longValue(), bucketInfo.stats.nObjects,
                    nativeToJH3(bucketInfo.stats.lastAccess), nativeToJH3(bucketInfo.stats.lastModification));

            return new JH3BucketInfo(nativeToJH3(bucketInfo.creation), stats);
        }

        return new JH3BucketInfo(nativeToJH3(bucketInfo.creation));
    }

    /**
     * Retrieve information about a bucket. No aggregate object information is produced. The status of the operation is
     * set and can be retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - A storage provider error
     *
     * @param bucketName The bucket name.
     * @return  The bucket information if the operation was successful, <code>null</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public JH3BucketInfo infoBucket(String bucketName) throws JH3Exception {
        return infoBucket(bucketName, false);
    }

    //public boolean forEachBucket(Object func, Object data) throws H3Exception {
    //    throw new H3Exception("Not supported");
    //}

    /* Object Management methods */

    /**
     * Create an object associated with a specific user (derived from the token). The bucket must exist and the object
     * should not. The object name must not exceed a certain size and conform to the following rules:
     * <ol>
     *     <li> May only consist of characters 0-9, a-z, A-Z, _ (underscore), / (slash), - (minus) and . (dot).
     *     <li> Must not start with a slash.
     *     <li> A slash must not be followed by another one.
     * </ol>
     * The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - The bucket doesn't exist or the user has no access.
     * <p>
     * {@link JH3Status#JH3_EXISTS} - The object already exists.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName The name of the bucket to host the object.
     * @param objectName The name of the object.
     * @param objectData The data of the object.
     * @return <code>true</code> is the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public boolean createObject(String bucketName, String objectName, JH3Object objectData) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);
        // Cannot allocate memory of zero size
        long s = objectData.getSize() > 0? objectData.getSize() : 1;
        Pointer data = new Memory(s);
        NativeLong size = new NativeLong(objectData.getSize());

        bucket.setString(0, bucketName);
        name.setString(0, objectName);
        // TODO check if there is a better way
        data.getByteBuffer(0, objectData.getSize()).put(objectData.getData());

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CreateObject(handle, token, bucket, name, data, size));
        return operationSucceeded(status);
    }

    /**
     * Copy a part of an object into a new one. Note that both objects will rest within the same bucket. The status of
     * the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access object or user has no access or new name is in use.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName        The name of the bucket to host the object.
     * @param srcObjectName     The name of the object to be copied.
     * @param offset            Offset with respect to the source object's first byte.
     * @param size              The amount of data to copy.
     * @param dstObjectName     The name of the new object.
     *
     * @return The number of bytes actually copied if is the operation was successful, <code>-1</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public long createObjectCopy(String bucketName, String srcObjectName, long offset, long size, String dstObjectName)
            throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer src = new Memory(srcObjectName.length() + 1);
        Pointer dst = new Memory(dstObjectName.length() + 1);
        NativeLong off = new NativeLong(offset);
        NativeLongByReference nSize = new NativeLongByReference( new NativeLong(size));

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CreateObjectCopy(handle, token, bucket, src, off, nSize, dst));
        if(operationSucceeded(status))
            return nSize.getValue().longValue();

        // operation failed
        return -1;
    }

    /**
     * Delete an object from specified bucket.The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to delete object or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName The bucket that hosts the object.
     * @param objectName The name of the object to be deleted.
     * @return <code>true</code> is the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception If an unknown status is received.
     */
    public boolean deleteObject(String bucketName, String objectName) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);

        bucket.setString(0,  bucketName);
        name.setString(0, objectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_DeleteObject(handle, token, bucket, name));
        return operationSucceeded(status);
    }


    /**
     * List objects that match a pattern. The pattern is a simple prefix rather than a regular expression. The pattern
     * must adhere to the object naming conventions. In case that there are more objects to be listed (indicated by the
     * operation status) the user may invoke again the function with an appropriately set offset in order to retrieve
     * the next batch of names. The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful (no more matching names exist).
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful (there could be more matching names).
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.

     * @param bucketName    The name of the bucket.
     * @param prefix        The initial part of the objects to be listed.
     * @param offset        The number of matching names to skip.
     * @return              The list of matching objects on success, <code>null</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public ArrayList<String> listObjects(String bucketName, String prefix, int offset) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer nPrefix = new Memory(prefix.length() + 1);
        PointerByReference objectArray = new PointerByReference();
        IntBuffer nObjects = IntBuffer.allocate(1);

        bucket.setString(0, bucketName);
        nPrefix.setString(0, prefix);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_ListObjects(handle, token, bucket, nPrefix, offset, objectArray, nObjects));

        // Return null on failure
        if(!operationSucceeded(status))
            return null;

        // Operation was successful
        ArrayList<String> objects = new ArrayList<>();
        for(int i = 0, arrayOffset  = 0; i < nObjects.get(0); i++) {
            String tmp = objectArray.getValue().getString(arrayOffset);
            objects.add(tmp);

            arrayOffset += tmp.length();
            while(objectArray.getValue().getByte(arrayOffset) == '\0')
                arrayOffset++;
        }

        return objects;
    }

    /**
     * List all objects in a bucket. In case that there are more objects to be listed (indicated by the
     * operation status) the user may invoke again the function with an appropriately set offset in order to retrieve
     * the next batch of names. The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful (no more matching names exist).
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful (there could be more matching names).
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.

     * @param bucketName    The name of the bucket.
     * @param offset        The number of matching names to skip.
     * @return              The list of matching objects on success, <code>null</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public ArrayList<String> listObjects(String bucketName, int offset) throws JH3Exception {
        return listObjects(bucketName, "", offset);
    }

    //public boolean forEachObject(String bucketName, String prefix, int nObjects, int offset,
    //                             Object func, Object data) throws H3Exception {
    //    throw new H3Exception("Not supported");
    //}

    /**
     * Get the object's information. Retrieve an object's size, timestamps, health status and creation.
     * The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful (there could be more matching names).
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName    The name of the bucket that hosts the object.
     * @param objectName    The name of the object.
     * @return              The object's information on success, <code>null</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public JH3ObjectInfo infoObject(String bucketName, String objectName) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);
        NativeObjectInfo info = new NativeObjectInfo();

        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_InfoObject(handle, token, bucket, name, info));
        if(operationSucceeded(status)){
            return new JH3ObjectInfo((info.isBad == 1), info.size.longValue(), nativeToJH3(info.creation),
                    nativeToJH3(info.lastAccess), nativeToJH3(info.lastModification),
                    nativeToJH3(info.lastChange), info.mode, info.uid, info.gid);
        }

        return null;
    }

    /**
     * Retrieve data from an object, starting at an offset. The size of the data to be retrieved cannot exceed
     * <code>MAX_INTEGER</code> due to Java's array restrictions. The status of the operation is set and can be retrieved
     * by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful, though more data are available.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access, or internal buffer allocation error.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName    The name bucket that hosts the object to be read.
     * @param objectName    The name of the object.
     * @param offset        The offset within the object's data.
     * @param size          Size of data to retrieve.
     * @return              The object's information on success, <code>null</code> otherwise.
     * @throws JH3Exception  If an unknown status is received, or requested size exceeds <code>MAX_INTEGER</code> size
     */
    public JH3Object readObject(String bucketName, String objectName, long offset, long size) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() +1);
        PointerByReference data;
        NativeLong nOffset = new NativeLong(offset);
        NativeLongByReference nSize = new NativeLongByReference( new NativeLong(size));

        bucket.setString(0, bucketName);
        name.setString(0, objectName);
        if(size != 0 )
            data =  new PointerByReference(new Memory(size));
        else
            data = new PointerByReference();

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_ReadObject(handle, token, bucket, name, nOffset, data, nSize));
        if(operationSucceeded(status)) {
            ByteBuffer buffer = data.getValue().getByteBuffer(0, nSize.getValue().longValue());
            byte[] array = new byte[buffer.remaining()];        // remaining returns int
            buffer.get(array);

            JH3Object obj = new JH3Object(array, nSize.getValue().longValue());

            // Free internal buffer that was allocated (only when size is zero)
            if(size == 0)
                Native.free(Pointer.nativeValue(data.getValue()));

            return obj;
        }
        return null;
    }

    /**
     * Retrieve full data from an object. Object must not exceed 2GB in order to retrieve it.The status of the
     * operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful, though more data are available.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access, or internal buffer allocation error.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName    The name bucket that hosts the object to be read.
     * @param objectName    The name of the object.
     * @return              The object's information on success, <code>null</code> otherwise.
     * @throws JH3Exception  If an unknown status is received, or requested size exceeds <code>MAX_INTEGER</code> size
     */
    public JH3Object readObject(String bucketName, String objectName) throws JH3Exception {
        return readObject(bucketName, objectName, 0, 0);
    }

    /**
     * Write an object associated with a specific user( derived from the token). If the object exists it
     * will be overwritten. Sparse objects are supported. The object name must not exceed a certain size and conform to
     * the following rules:
     * <ol>
     *  <li> May only consist of characters 0-9, a-z, A-Z, _ (underscore). / (slash) , - (minus) and . (dot).
     *  <li> Must not start with a slash.
     *  <li> A slash must not be followed by another one.
     * </ol>
     *
     * The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    objectName         The name of the object to be written.
     * @param    objectData         The object's data
     * @param    offset             Offset from the object's first byte.
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean writeObject(String bucketName, String objectName, JH3Object objectData, long offset) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer name = new Memory(objectName.length() + 1);
        // Cannot allocate memory of zero size
        long s = objectData.getSize() > 0? objectData.getSize() : 1;
        Pointer data = new Memory(s);
        NativeLong nOffset= new NativeLong(offset);
        NativeLong nSize = new NativeLong(objectData.getSize());
        bucket.setString(0, bucketName);
        name.setString(0, objectName);
        data.getByteBuffer(0, objectData.getSize()).put(objectData.getData());

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_WriteObject(handle, token, bucket, name, data, nSize, nOffset));
        return operationSucceeded(status);
    }

    /**
     * Write an object associated with a specific user( derived from the token), without using an offset. If the object
     * exists it will be overwritten. Sparse objects are supported. The object name must not exceed a certain size and
     * conform to the following rules:
     * <ol>
     *  <li> May only consist of characters 0-9, a-z, A-Z, _ (underscore). / (slash) , - (minus) and . (dot).
     *  <li> Must not start with a slash.
     *  <li> A slash must not be followed by another one.
     * </ol>
     *
     * The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    objectName         The name of the object to be written.
     * @param    objectData         The object's data
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean writeObject(String bucketName, String objectName, JH3Object objectData) throws JH3Exception {
        return writeObject(bucketName, objectName, objectData, 0);
    }

    /**
     * Copy a part of an object into a new or existing one, at a user provided offset.
     * Note that both objects will rest within the same bucket. The status of the operation is set and can be
     * retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    srcObjectName      The name of the object to be copied.
     * @param    dstObjectName      The name of the object to be written.
     * @param    srcOffset          Offset with respect to the source object's first byte.
     * @param    dstOffset          Offset with respect to the destination object's first byte.
     * @param    size               The amount of data to copy.
     *
     * @return  The number of bytes actually copied if the operation was successful, <code>-1</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public long writeObjectCopy(String bucketName, String srcObjectName, long srcOffset,
                                   long size, String dstObjectName,  long dstOffset) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1 );
        Pointer src = new Memory(srcObjectName.length() +1);
        Pointer dst = new Memory(dstObjectName.length() +1);
        NativeLong srcOff = new NativeLong(srcOffset);
        NativeLongByReference nSize = new NativeLongByReference(new NativeLong(size));
        NativeLong dstOff = new NativeLong(dstOffset);

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_WriteObjectCopy(handle, token, bucket, src, srcOff, nSize, dst, dstOff));
        if(operationSucceeded(status))
            return nSize.getValue().longValue();

        // operation failed
        return -1;
    }

    /**
     * Copies an object provided the new name is not taken by another object unless it is explicitly allowed
     * by the user in which case the previous object will be overwritten. Note that both objects will rest within
     * the same bucket. The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access, or destination name is in use and
     * not allowed to overwrite.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    srcObjectName      The name of the object to be copied.
     * @param    dstObjectName      The name to be written.
     * @param    noOverwrite        Overwrite flag.
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean copyObject(String bucketName, String srcObjectName, String dstObjectName,
                              boolean noOverwrite) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer src = new Memory(srcObjectName.length() +1);
        Pointer dst = new Memory(dstObjectName.length() +1);
        byte overwrite = (byte) (noOverwrite? 1:0);        // map boolean to byte

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CopyObject(handle, token, bucket, src, dst, overwrite));
        return operationSucceeded(status);
    }

    /**
     * Copies an object provided to the destination name, always allowing overwrite, even if the destination object
     * already exists.Note that both objects will rest within the same bucket. The status of the operation is set and
     * can be retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    srcObjectName      The name of the object to be copied.
     * @param    dstObjectName      The name to be written.
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean copyObject(String bucketName, String srcObjectName, String dstObjectName) throws JH3Exception {
        return copyObject(bucketName, srcObjectName, dstObjectName, false);
    }

    /**
     * Renames an object provided the destination name is not taken by another object unless it is explicitly allowed
     * by the user in which case the previous object will be overwritten. Note that both objects will rest within
     * the same bucket.  The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access, or destination name is in use and
     * not allowed to overwrite.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_EXISTS} - The destination object exists and we are not allowed to replace it.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    srcObjectName      The name of the object to be renamed.
     * @param    dstObjectName      The destination name to be assumed by the object.
     * @param    noOverwrite        Overwrite flag.
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean moveObject(String bucketName, String srcObjectName, String dstObjectName,
                              boolean noOverwrite) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer src = new Memory(srcObjectName.length() +1);
        Pointer dst = new Memory(dstObjectName.length() + 1);
        byte overwrite = (byte) (noOverwrite? 1:0);

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_MoveObject(handle, token, bucket, src, dst, overwrite));
        return operationSucceeded(status);

    }

    /**
     * Renames an object to the destination name, always allowing overwrite, even if the destination object already
     * exists. Note that both objects will rest within the same bucket. The status of the operation is set and can be
     * retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access, or destination name is in use and
     * not allowed to overwrite.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_EXISTS} - The destination object exists and we are not allowed to replace it.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    srcObjectName      The name of the object to be renamed.
     * @param    dstObjectName      The destination name to be assumed by the object.
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean moveObject(String bucketName, String srcObjectName, String dstObjectName) throws JH3Exception {
        return moveObject(bucketName, srcObjectName, dstObjectName, false);
    }

    /**
     * Reduces the size of the object by permanently removing excess data. The status of the operation is set and can be
     * retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to retrieve object info or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName            The name of the bucket hosting the object.
     * @param objectName            The name of the object to be truncated.
     * @param size                  Size of truncated object. If the object was previously larger than this size, the
     *                              extra data is lost. If the object was previously shorter, it is extended, and the
     *                              extended part reads as null bytes ('\0').
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean truncateObject(String bucketName, String objectName, long size) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer name = new Memory(objectName.length() +1);
        NativeLong nSize = new NativeLong(size);

        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_TruncateObject(handle, token, bucket, name, nSize));

        return operationSucceeded(status);
    }

    /**
     * Reduces the size of the object to zero by permanently removing all data in object. The status of the operation
     * is set and can be retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to retrieve object info or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName            The name of the bucket hosting the object.
     * @param objectName            The name of the object to be truncated.

     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean truncateObject(String bucketName, String objectName) throws JH3Exception {
        return truncateObject(bucketName, objectName, 0);
    }

    /**
     * Swaps data between two objects. Note that both objects will rest within the same bucket. The status of the
     * operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to retrieve object info or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName            The name of the bucket hosting the objects.
     * @param srcObjectName         The name of the first object to be exchanged.
     * @param dstObjectName         The name of the second object to be exchanged.
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean exchangeObject(String bucketName, String srcObjectName, String dstObjectName) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer firstName = new Memory(srcObjectName.length() + 1);
        Pointer secondName = new Memory(dstObjectName.length() + 1);

        bucket.setString(0, bucketName);
        firstName.setString(0, srcObjectName);
        secondName.setString(0, dstObjectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_ExchangeObject(handle, token, bucket, firstName, secondName));

        return operationSucceeded(status);
    }

    
     /**
     * Set an object's permissions attribute (used by h3fuse). The status of the
     * operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to update object info or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_NAME_TOO_LONG} - The bucket or object name is too long.
     * 
     * @param bucketName            The name of the bucket hosting the object.
     * @param objectName            The name of the object.
     * @param mode                  The objects permissions.
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean setObjectPermissions(String bucketName, String objectName, int mode) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);

        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        NativeAttribute attr = new NativeAttribute();
        attr.type = JH3AttributeType.JH3_ATTRIBUTE_PERMISSIONS.ordinal();
        attr.unionval.mode = mode;

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_SetObjectAttributes(handle, token, bucket, name, attr));

        return operationSucceeded(status);
    }


  /**
     * Set an object's owner attribute (used by h3fuse). The status of the
     * operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to update object info or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     * <p>
     * {@link JH3Status#JH3_NAME_TOO_LONG} - The bucket or object name is too long.
     * 
     * @param bucketName            The name of the bucket hosting the object.
     * @param objectName            The name of the object.
     * @param uid                   The user id.
     * @param gid                   The group id.
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean setObjectOwner(String bucketName, String objectName, int uid, int gid) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);
        
        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        NativeAttribute attr = new NativeAttribute();
        attr.type = JH3AttributeType.JH3_ATTRIBUTE_OWNER.ordinal();
        attr.unionval.structval.uid = uid;
        attr.unionval.structval.gid = gid;

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_SetObjectAttributes(handle, token, bucket, name, attr));

        return operationSucceeded(status);
    }

    /* Multipart Management methods */

    /**
     * Initiate a multipart object associated with a specific user(derived from the token). Though the bucket must exist
     * there are no checks for the object itself since they are performed during completion. Once initiated the object
     * is accessed  through its ID rather than its name. The object name must not exceed a certain size and conform to
     * the following rules:
     * <ol>
     *  <li> May only consist of characters 0-9, a-z, A-Z, _ (underscore). / (slash) , - (minus) and . (dot).
     *  <li> Must not start with a slash.
     *  <li> A slash must not be followed by another one.
     *</ol>
     *
     * The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    bucketName         The name of the bucket to host the object.
     * @param    objectName         The name of the object to be created.
     *
     * @return              The multipart ID if the operation was successful, <code>null</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public JH3MultipartId createMultipart(String bucketName, String objectName) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer name = new Memory(objectName.length() +1);
        PointerByReference multipartId = new PointerByReference();
        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CreateMultipart(handle, token, bucket, name, multipartId));

        // Return null on error
        if(!operationSucceeded(status)) {
            return null;
        }

        return new JH3MultipartId(multipartId.getValue().getString(0));
    }

    /**
     * Converts a multipart object into an ordinary one by coalescing uploaded parts ordered by their part-ID.
     * If another object with that name already exists it is overwritten. Once completed, the multipart-ID is
     * invalidated thus the multipart API becomes in-applicable for the object. The status of the operation is set and
     * can be retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The multipart object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - The user has no access or unable to access multipart object, or no parts have been
     * uploaded.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param   multipartId         The id corresponding to the multipart object.
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean completeMultipart(JH3MultipartId multipartId) throws JH3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        multipart.setString(0, multipartId.getMultipartId());

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CompleteMultipart(handle, token, multipart));
        return operationSucceeded(status);
    }

    /**
     * Deletes a multipart object along with all uploaded parts if any. Once deleted, the multipart-ID is
     * invalidated. The status of the operation is set and can be retrieved by {@link JH3#getStatus() getStatus()}.
     * Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The multipart object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - The user has no access or unable to access multipart object.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param    multipartId        The id corresponding to the multipart object.
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean abortMultipart(JH3MultipartId multipartId) throws JH3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        multipart.setString(0, multipartId.getMultipartId());

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_AbortMultipart(handle, token, multipart));
        return operationSucceeded(status);
    }

    /**
     * Retrieve the ID of all multipart objects in specified bucket. In case that there are more multipart objects to
     * be listed (indicated by the operation status) the user may invoke again the function with an appropriately set
     * offset in order to retrieve the next batch of multiparts. The status of the operation is set and can be
     * retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful (no more matching names exist).
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful (there could be more matching names).
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName        The name of the bucket.
     * @param offset            The number of multipart IDs to skip.
     *
     * @return                  The list of multipart objects if the operation was successful, <code>null</code>
     * otherwise.
     * @throws JH3Exception      If an unknown status is received.
     */
    public ArrayList<JH3MultipartId> listMultiparts(String bucketName, int offset) throws JH3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        IntBuffer nIds = IntBuffer.allocate(1);
        PointerByReference multipartIdArray = new PointerByReference();

        bucket.setString(0, bucketName);

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_ListMultiparts(handle, token, bucket, offset, multipartIdArray, nIds));
        // Return null on failure
        if(!operationSucceeded(status))
            return null;

        ArrayList<JH3MultipartId> list = new ArrayList<>();
        int off = 0;
        for (int i = 0; i < nIds.get(0); i++){
            String tmp = multipartIdArray.getValue().getString(off);
            off += tmp.length() + 1;     // Also skip null terminated character
            list.add(new JH3MultipartId(tmp));
        }

        // Free internal buffer
        Native.free(Pointer.nativeValue(multipartIdArray.getValue()));
        return list;
    }

    /**
     * Retrieve the ID of all multipart objects in specified bucket. In case that there are more multipart objects to
     * be listed (indicated by the operation status) the user may invoke
     * {@link JH3#listMultiparts(String,int) listMultiparts(String bucketName, int offset)} with an appropriately set
     * offset in order to retrieve the next batch of multiparts. The status of the operation is set and can be
     * retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful (no more matching names exist).
     * <p>
     * {@link JH3Status#JH3_CONTINUE} - The operation was successful (there could be more matching names).
     * <p>
     * {@link JH3Status#JH3_FAILURE} - Unable to access bucket or user has no access.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The bucket doesn't exist.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param bucketName        The name of the bucket.
     *
     * @return                  The list of multipart objects if the operation was successful, <code>null</code>
     * otherwise.
     * @throws JH3Exception      If an unknown status is received.
     */
    public ArrayList<JH3MultipartId> listMultiparts(String bucketName) throws JH3Exception {
        return listMultiparts(bucketName, 0);
    }

    /**
     * Retrieves information for each part of a multipart object. The status of the operation is set and can be
     * retrieved by {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The multipart object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - The user has no access or unable to access multipart object.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param   multipartId        The id corresponding to the multipart object.
     *
     * @return                  The list of parts associated with the multipart ID,  <code>null</code> otherwise.
     * @throws JH3Exception      If an unknown status is received.
     */
    public ArrayList<JH3PartInfo> listParts(JH3MultipartId multipartId) throws JH3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() + 1);
        IntBuffer nParts = IntBuffer.allocate(1);
        multipart.setString(0, multipartId.getMultipartId());
        PointerByReference partInfoPointer = new PointerByReference();

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_ListParts(handle, token, multipart, partInfoPointer, nParts));
        if(!operationSucceeded(status))
            return null;

        // This can be extremely slow for large arrays
        // https://www.eshayne.com/jnaex/index.html?example=7
        NativePartInfo partInfo = new NativePartInfo(partInfoPointer.getValue());
        partInfo.read();
        NativePartInfo[] partInfoArray = (NativePartInfo[])  partInfo.toArray(nParts.get(0));

        ArrayList<JH3PartInfo> list = new ArrayList<>();
        for(int i = 0; i < nParts.get(0); i++) {
            list.add(new JH3PartInfo(partInfoArray[i].partNumber, partInfoArray[i].size.longValue()));
        }

        // Free internal buffer
        Native.free(Pointer.nativeValue(partInfoPointer.getValue()));

        return list;

    }

    /**
     * Creates a part of a multipart object designated by a number. If a part with the same number exists it is
     * replaced by the new one. The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The multipart object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - The user has no access or unable to access multipart object.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param   objectData         The data to be written.
     * @param   multipartId        The ID of the multipart object.
     * @param   partNumber         The part number
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean createPart(JH3Object objectData, JH3MultipartId multipartId, int partNumber) throws JH3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        // Cannot allocate memory of zero size
        long s = objectData.getSize() > 0? objectData.getSize() : 1;
        Pointer data = new Memory(s);
        NativeLong size = new NativeLong(objectData.getSize());

        multipart.setString(0, multipartId.getMultipartId());
        data.getByteBuffer(0, objectData.getSize()).put(objectData.getData());

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CreatePart(handle, token, multipart, partNumber, data, size));
        return operationSucceeded(status);
    }

    /**
     * Creates a part of a multipart object from a pre-existing object. If a part with the same number exists it is
     * replaced by the new one. The data is sourced from an ordinary object expected to be hosted in the same  bucket
     * as the multipart one. The status of the operation is set and can be retrieved by
     * {@link JH3#getStatus() getStatus()}. Expected status from operation:
     * <p>
     * {@link JH3Status#JH3_SUCCESS} - The operation was successful.
     * <p>
     * {@link JH3Status#JH3_NOT_EXISTS} - The multipart object doesn't exist.
     * <p>
     * {@link JH3Status#JH3_FAILURE} - The user has no access or unable to access multipart object.
     * <p>
     * {@link JH3Status#JH3_INVALID_ARGS} - The operation has missing or malformed arguments.
     *
     * @param   objectName         The name of the object to be copied.
     * @param   offset             The offset with respect to the source object's first byte.
     * @param   size               The size of the data to be copied.
     * @param   multipartId        The ID of the multipart object.
     * @param   partNumber         The part number
     *
     * @return              <code>true</code> if the operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status is received.
     */
    public boolean createPartCopy(String objectName, long offset, long size,
                                  JH3MultipartId multipartId, int partNumber) throws JH3Exception {
        Pointer name = new Memory(objectName.length() +1);
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        NativeLong nOffset = new NativeLong(offset);
        NativeLong nSize = new NativeLong(size);

        name.setString(0, objectName);
        multipart.setString(0, multipartId.getMultipartId());

        status = JH3Status.fromInt(JH3Interface.INSTANCE.H3_CreatePartCopy(handle, token, name, nOffset, nSize, multipart, partNumber));
        return operationSucceeded(status);
    }

    /**
     * Helper function to convert a native timespec to a JH3Timespec.
     * @param timespec      The native timespec
     * @return              A JH3Timespec
     */
    private JH3Timespec nativeToJH3(NativeTimespec timespec){
        return new JH3Timespec(timespec.tv_sec.longValue(), timespec.tv_nsec.longValue());
    }

    /**
     * Checks if operation has succeeded, based on the operation's status.
     * @param status        The status of the operation to be checked.
     * @return              <code>true</code> if operation was successful, <code>false</code> otherwise.
     * @throws JH3Exception  If an unknown status was received.
     */
    private boolean operationSucceeded(JH3Status status) throws JH3Exception {
        switch (status) {
            case JH3_FAILURE:
            case JH3_INVALID_ARGS:
            case JH3_STORE_ERROR:
            case JH3_EXISTS:
            case JH3_NOT_EXISTS:
            case JH3_NAME_TOO_LONG:
            case JH3_NOT_EMPTY:
                return false;
            case JH3_SUCCESS:
            case JH3_CONTINUE:
                return true;
            default:
                throw new JH3Exception("Received unknown status: " + status);
        }
    }

}
