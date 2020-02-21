package gr.forth.ics.JH3lib;

import com.sun.jna.Memory;
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
 * Wrapper of JH3libInterface to make JH3lib interface more Java-like
 */
public class JH3Client implements Serializable{

    private static final Cleaner cleaner = Cleaner.create();
    private Pointer handle;
    private H3Auth token;
    private H3Status status = H3Status.H3_SUCCESS;
    public static int H3_BUCKET_NAME_SIZE = JH3libInterface.H3_BUCKET_NAME_SIZE;
    public static int H3_OBJECT_NAME_SIZE = JH3libInterface.H3_OBJECT_NAME_SIZE;

    /**
     * If empty constructor is called, user must call init
     */
    public JH3Client(){ }

    //public JH3Client(H3StoreType storeType, String config, int userId){
    //    this.init(storeType, config, userId);
    //}
    //// TODO Consider if this is needed
    //public void free() {
    //    JH3libInterface.INSTANCE.H3_Free(handle);
    //    handle = null;      // handle is no longer valid
    //}
    // }

    public JH3Client(H3StoreType storeType, String config, int userId){
        Pointer configPath = new Memory(config.length() + 1);
        configPath.setString(0, config);
        Logger log = Logger.getLogger(JH3Client.class.getName());
        handle = JH3libInterface.INSTANCE.H3_Init(storeType.getStoreType(), configPath);
        token = new H3Auth(userId);

        // When object is garbage collected, H3_Free is called to free the handle
        cleaner.register(this, () -> {log.info("Calling H3_Free"); JH3libInterface.INSTANCE.H3_Free(handle);});
    }

    public String getVersion(){ return JH3libInterface.INSTANCE.H3_Version().getString(0); }

    public H3Status getStatus() { return status; }

    public String getStatusName(){ return status.name(); }



    /* Bucket Management methods */

    public boolean createBucket(String bucketName) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        bucket.setString(0, bucketName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreateBucket(handle, token, bucket));
        return operationSucceeded(status);
    }

    public boolean deleteBucket(String bucketName) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        bucket.setString(0, bucketName);

        status = H3Status.fromInt((JH3libInterface.INSTANCE.H3_DeleteBucket(handle, token, bucket)));
        return operationSucceeded(status);
    }

    public ArrayList<String> listBuckets() throws H3Exception {
        PointerByReference bucketNameArray = new PointerByReference();
        IntBuffer nBuckets = IntBuffer.allocate(1);

        // Retrieve bucket names
        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_ListBuckets(handle, token, bucketNameArray, nBuckets));

        // Return empty list on failure
        if(!operationSucceeded(status))
            return new ArrayList<>();


        // Operation was successful
        ArrayList<String> buckets = new ArrayList<>();
        for(int i = 0, arrayOffset  = 0; i < nBuckets.get(0); i++) {
            String tmp = bucketNameArray.getValue().getString(arrayOffset);
            buckets.add(tmp);
            arrayOffset += tmp.length();
            while(bucketNameArray.getValue().getByte(arrayOffset) == '\0')
                arrayOffset++;
        }
        return buckets;
    }

    public H3BucketInfo infoBucket(String bucketName, boolean getStats) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        NativeBucketInfo bucketInfo= new NativeBucketInfo();
        byte retrieveStats = (byte) (getStats? 1:0);        // map boolean to byte

        bucket.setString(0, bucketName);
        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_InfoBucket(handle, token, bucket, bucketInfo, retrieveStats));
        if (!operationSucceeded(status))
            return null;

        if(getStats){
            H3BucketStats stats = new H3BucketStats(bucketInfo.stats.size.longValue(), bucketInfo.stats.nObjects,
                    bucketInfo.stats.lastAccess.longValue(), bucketInfo.stats.lastModification.longValue());
            return new H3BucketInfo(bucketInfo.creation.longValue(), stats);
        }

        return new H3BucketInfo(bucketInfo.creation.longValue());
    }

    public H3BucketInfo infoBucket(String bucketName) throws H3Exception {
        return infoBucket(bucketName, false);
    }

    //public boolean forEachBucket(Object func, Object data) throws H3Exception {
    //    throw new H3Exception("Not supported");
    //}

    /* Object Management methods */

    public boolean createObject(String bucketName, String objectName, H3Object object) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);
        Pointer data = new Memory(object.getSize());
        NativeLong size = new NativeLong(object.getSize());

        bucket.setString(0, bucketName);
        name.setString(0, objectName);
        // TODO check if there is a better way
        data.getByteBuffer(0, object.getSize()).put(object.getData());

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreateObject(handle, token, bucket, name, data, size));
        return operationSucceeded(status);
    }

    public boolean createObjectCopy(String bucketName, String srcObjectName, long offset, long size, String dstObjectName) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer src = new Memory(srcObjectName.length() + 1);
        Pointer dst = new Memory(dstObjectName.length() + 1);
        NativeLong off = new NativeLong(offset);
        NativeLong s = new NativeLong(size);

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreateObjectCopy(handle, token, bucket, src, off, s, dst));
        return  operationSucceeded(status);
    }

    public boolean deleteObject(String bucketName, String objectName) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);

        bucket.setString(0,  bucketName);
        name.setString(0, objectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_DeleteObject(handle, token, bucket, name));
        return operationSucceeded(status);
    }

    public ArrayList<String> listObjects(String bucketName, String prefix, int offset) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer nPrefix = new Memory(prefix.length() + 1);
        PointerByReference objectArray = new PointerByReference();
        IntBuffer nObjects = IntBuffer.allocate(1);

        bucket.setString(0, bucketName);
        nPrefix.setString(0, prefix);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_ListObjects(handle, token, bucket, nPrefix, offset, objectArray, nObjects));

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

    public ArrayList<String> listObjects(String bucketName) throws H3Exception {
        return listObjects(bucketName, "", 0);
    }

    //public boolean forEachObject(String bucketName, String prefix, int nObjects, int offset,
    //                             Object func, Object data) throws H3Exception {
    //    throw new H3Exception("Not supported");
    //}

    public H3ObjectInfo infoObject(String bucketName, String objectName) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() + 1);
        NativeObjectInfo info = new NativeObjectInfo();

        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_InfoObject(handle, token, bucket, name, info));
        if(operationSucceeded(status)){
            return new H3ObjectInfo((info.isBad == 1), info.size.longValue(), info.creation.longValue(),
                    info.lastAccess.longValue(), info.lastModification.longValue());
        }

        return null;
    }

    public H3Object readObject(String bucketName, String objectName, long offset,long size) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer name = new Memory(objectName.length() +1);
        PointerByReference data;
        NativeLong nOffset = new NativeLong(offset);
        NativeLongByReference nSize = new NativeLongByReference( new NativeLong(size));

        bucket.setString(0, bucketName);
        name.setString(0, objectName);
        if(size > 0 )
            data =  new PointerByReference(new Memory(size));
        else
            data = new PointerByReference();

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_ReadObject(handle, token, bucket, name, nOffset, data, nSize));
        if(operationSucceeded(status)) {
            ByteBuffer buffer = data.getValue().getByteBuffer(0, nSize.getValue().longValue());
            byte[] array = new byte[buffer.remaining()];        // remaining returns int
            buffer.get(array);

            H3Object obj = new H3Object(array, nSize.getValue().longValue());
            return obj;
        }
        return null;
    }

    public H3Object readObject(String bucketName, String objectName) throws H3Exception{
        return readObject(bucketName, objectName, 0, 0);
    }

    public boolean writeObject(String bucketName, String objectName, H3Object object, long offset) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer name = new Memory(objectName.length() + 1);
        Pointer data = new Memory(object.getSize());
        NativeLong nOffset= new NativeLong(offset);
        NativeLong nSize = new NativeLong(object.getSize());
        bucket.setString(0, bucketName);
        name.setString(0, objectName);
        data.getByteBuffer(0, object.getSize()).put(object.getData());

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_WriteObject(handle, token, bucket, name, data, nSize, nOffset));
        return operationSucceeded(status);
    }

    public boolean writeObject(String bucketName, String objectName, H3Object object) throws H3Exception {
        return writeObject(bucketName, objectName, object, 0);
    }

    public boolean writeObjectCopy(String bucketName, String srcObjectName, long srcOffset,
                                   long size, String dstObjectName,  long dstOffset) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() +1 );
        Pointer src = new Memory(srcObjectName.length() +1);
        Pointer dst = new Memory(dstObjectName.length() +1);
        NativeLong srcOff = new NativeLong(srcOffset);
        NativeLong nSize = new NativeLong(size);
        NativeLong dstOff = new NativeLong(dstOffset);

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_WriteObjectCopy(handle, token, bucket, src, srcOff, nSize, dst, dstOff));
        return operationSucceeded(status);
    }

    public boolean copyObject(String bucketName, String srcObjectName, String dstObjectName,
                              boolean noOverwrite) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() + 1);
        Pointer src = new Memory(srcObjectName.length() +1);
        Pointer dst = new Memory(dstObjectName.length() +1);
        byte overwrite = (byte) (noOverwrite? 1:0);        // map boolean to byte

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CopyObject(handle, token, bucket, src, dst, overwrite));
        return operationSucceeded(status);
    }

    public boolean copyObject(String bucketName, String srcObjectName, String dstObjectName) throws H3Exception {
        return copyObject(bucketName, srcObjectName, dstObjectName, false);
    }

    public boolean moveObject(String bucketName, String srcObjectName, String dstObjectName,
                              boolean noOverwrite) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer src = new Memory(srcObjectName.length() +1);
        Pointer dst = new Memory(dstObjectName.length() + 1);
        byte overwrite = (byte) (noOverwrite? 1:0);

        bucket.setString(0, bucketName);
        src.setString(0, srcObjectName);
        dst.setString(0, dstObjectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_MoveObject(handle, token, bucket, src, dst, overwrite));
        return operationSucceeded(status);

    }

    public boolean moveObject(String bucketName, String srcObjectName, String dstObjectName) throws H3Exception {
        return moveObject(bucketName, srcObjectName, dstObjectName, false);
    }

    /* Multipart Management methods */

    public H3MultipartId createMultipart(String bucketName, String objectName) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        Pointer name = new Memory(objectName.length() +1);
        PointerByReference multipartId = new PointerByReference();
        bucket.setString(0, bucketName);
        name.setString(0, objectName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreateMultipart(handle, token, bucket, name, multipartId));

        // Return null on error
        if(!operationSucceeded(status)) {
            return null;
        }

        return new H3MultipartId(multipartId.getValue().getString(0));
    }

    public boolean completeMultipart(H3MultipartId multipartId) throws H3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        multipart.setString(0, multipartId.getMultipartId());

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CompleteMultipart(handle, token, multipart));
        return operationSucceeded(status);
    }

    public boolean abortMultipart(H3MultipartId multipartId) throws H3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        multipart.setString(0, multipartId.getMultipartId());

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_AbortMultipart(handle, token, multipart));
        return operationSucceeded(status);
    }

    public ArrayList<String> listMultiparts(String bucketName, int offset) throws H3Exception {
        Pointer bucket = new Memory(bucketName.length() +1);
        IntBuffer nIds = IntBuffer.allocate(1);
        PointerByReference multipartIdArray = new PointerByReference();

        bucket.setString(0, bucketName);

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_ListMultiparts(handle, token, bucket, offset, multipartIdArray, nIds));
        // Return null on failure
        if(!operationSucceeded(status))
            return null;

        ArrayList<String> list = new ArrayList<>();
        int off = 0;
        for (int i = 0; i < nIds.get(0); i++){
            String tmp = multipartIdArray.getValue().getString(off);
            off += tmp.length() + 1;     // Also skip null terminated character
            list.add(tmp);
        }

        return list;
    }

    public ArrayList<NativePartInfo> listParts(H3MultipartId multipartId) throws H3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() + 1);
        IntBuffer nParts = IntBuffer.allocate(1);
        multipart.setString(0, multipartId.getMultipartId());
        NativePartInfo.ByReference[] partInfoArray = null;

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_ListParts(handle, token, multipart, partInfoArray, nParts));
        if(!operationSucceeded(status))
            return null;

        ArrayList<NativePartInfo> list = new ArrayList<>();
        for(int i = 0; i < nParts.get(0); i++) {
            list.add(partInfoArray[i]);
        }

        return list;

    }

    public boolean createPart(H3Object object, H3MultipartId multipartId, int partNumber) throws H3Exception {
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        Pointer data = new Memory(object.getSize());
        NativeLong size = new NativeLong(object.getSize());

        multipart.setString(0, multipartId.getMultipartId());
        data.getByteBuffer(0, object.getSize()).put(object.getData());

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreatePart(handle, token, multipart, partNumber, data, size));
        return operationSucceeded(status);
    }

    public boolean createPartCopy(String objectName, long offset, long size,
                                  H3MultipartId multipartId, int partNumber) throws H3Exception {
        Pointer name = new Memory(objectName.length() +1);
        Pointer multipart = new Memory(multipartId.getMultipartId().length() +1);
        NativeLong nOffset = new NativeLong(offset);
        NativeLong nSize = new NativeLong(size);

        name.setString(0, objectName);
        multipart.setString(0, multipartId.getMultipartId());

        status = H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreatePartCopy(handle, token, name, nOffset, nSize, multipart, partNumber));
        return operationSucceeded(status);
    }


    private boolean operationSucceeded(H3Status status) throws H3Exception {
        switch (status) {
            case H3_FAILURE:
            case H3_INVALID_ARGS:
            case H3_STORE_ERROR:
            case H3_EXISTS:
            case H3_NOT_EXISTS:
                return false;
            case H3_SUCCESS:
            case H3_CONTINUE:
                return true;
            default:
                throw new H3Exception("Received unknown status: " + status);
        }
    }

}

