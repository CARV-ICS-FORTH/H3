package gr.forth.ics.JH3lib;

import com.sun.jna.*;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;

import java.nio.IntBuffer;

/**
 * Interface that maps h3lib's native functions to Java
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public interface JH3libInterface extends Library {
    // public static final String JNA_LIBRARY_NAME = "/usr/local/lib/libH3.so";
    // public static final NativeLibrary JNA_NATIVE_LIB = NativeLibrary.getInstance(JH3libInterface.JNA_LIBRARY_NAME);
    // public static final JH3libInterface INSTANCE = (JH3libInterface) Native.load(JH3libInterface.JNA_LIBRARY_NAME, JH3libInterface.class);

    // Might just use this
    JH3libInterface INSTANCE = Native.load("h3lib", JH3libInterface.class);

    // Defines of H3
    int H3_BUCKET_NAME_SIZE = 64;     //!< Maximum number of characters allowed for a bucket
    int H3_OBJECT_NAME_SIZE = 512;     //!< Maximum number of characters allowed for an object

    /**
     * Storage providers supported by H3;  Represents values of H3_StoreType enum
     * */
    int H3_STORE_CONFIG = 0;         //!< Provider is set in the configuration file
    int H3_STORE_FILESYSTEM = 1;     //!< Mounted filesystem
    int H3_STORE_KREON = 2;          //!< Kreon cluster  (not available)
    int H3_STORE_ROCKSDB = 3;        //!< RocksDB server (not available)
    int H3_STORE_REDIS = 4;          //!< Redis cluster  (not available)
    int H3_STORE_IME = 5;            //!< IME cluster    (not available)
    int H3_STORE_NumOfStores = 6;    //!< Not an option, used for iteration purposes.

    /**
     * Status / error codes supported by H3; Represents values of H3_Status enum
     */
    int H3_FAILURE = 0;          //!< Operation failed
    int H3_INVALID_ARGS = 1;     //!< Arguments are missing or malformed
    int H3_STORE_ERROR = 2;      //!< External (store provider) error
    int H3_EXISTS = 3;           //!< Bucket or object already exists
    int H3_NOT_EXISTS = 4;       //!< Bucket or object does not exist
    int H3_SUCCESS = 5;          //!< Operation succeeded
    int H3_CONTINUE = 6;         //!< Operation succeeded though there are more data to retrieve

    // User function to be invoked for each bucket
    interface h3_name_iterator_cb extends Callback {
        void apply(Pointer name, Pointer userData);
    }

    // Return the version string
    Pointer H3_Version();


    // Handle Management
    Pointer H3_Init(int storageType, Pointer cfgFileName);
    void H3_Free(Pointer handle);

    // Bucket Management
    int H3_CreateBucket(Pointer handle, H3Auth token, Pointer bucketName);
    int H3_DeleteBucket(Pointer handle, H3Auth token, Pointer bucketName);
    int H3_ListBuckets(Pointer handle, H3Auth token, PointerByReference bucketNameArray, IntBuffer nBuckets);
    int H3_ForeachBucket(Pointer handle, H3Auth token, JH3libInterface.h3_name_iterator_cb function, Pointer userData);
    int H3_InfoBucket(Pointer handle, H3Auth token, Pointer bucketName, NativeBucketInfo bucketInfo, byte getStats);

    // Object Management
    int H3_CreateObject(Pointer handle, H3Auth token, Pointer bucketName, Pointer objectName, Pointer data, NativeLong size);
    int H3_CreateObjectCopy(Pointer handle, H3Auth token, Pointer bucketName, Pointer srcObjectName, NativeLong offset, NativeLong size, Pointer dstObjectName);
    int H3_DeleteObject(Pointer handle, H3Auth token,  Pointer bucketName, Pointer objectName);
    int H3_ListObjects(Pointer handle, H3Auth token, Pointer bucketName, Pointer prefix, int offset, PointerByReference objectNameArray, IntBuffer nObjects);
    int H3_ForeachObject(Pointer handle, H3Auth token,  Pointer bucketName, Pointer prefix, int nObjects, int offset, JH3libInterface.h3_name_iterator_cb function, Pointer userData);
    int H3_InfoObject(Pointer handle, H3Auth token, Pointer bucketName, Pointer objectName, NativeObjectInfo objectInfo);
    int H3_ReadObject(Pointer handle, H3Auth token, Pointer bucketName, Pointer objectName, NativeLong offset, PointerByReference data, NativeLongByReference size);
    int H3_WriteObject(Pointer handle, H3Auth token, Pointer bucketName, Pointer objectName, Pointer data, NativeLong size, NativeLong offset);
    int H3_WriteObjectCopy(Pointer handle, H3Auth token, Pointer bucketName, Pointer srcObjectName, NativeLong srcOffset, NativeLong size, Pointer dstObjectName, NativeLong dstOffset);
    int H3_CopyObject(Pointer handle, H3Auth token, Pointer bucketName, Pointer srcObjectName, Pointer dstObjectName, byte noOverwrite);
    int H3_MoveObject(Pointer handle, H3Auth token, Pointer bucketName, Pointer srcObjectName, Pointer dstObjectName, byte noOverwrite);

    // Multipart Management
    int H3_CreateMultipart(Pointer handle, H3Auth token, Pointer bucketName, Pointer objectName, PointerByReference multipartId);
    int H3_CompleteMultipart(Pointer handle, H3Auth token, Pointer multipartId);
    int H3_AbortMultipart(Pointer handle, H3Auth token, Pointer multipartId);
    int H3_ListMultiparts(Pointer handle, H3Auth token, Pointer bucketName, int offset, PointerByReference multipartIdArray, IntBuffer nIds);
    int H3_ListParts(Pointer handle, H3Auth token, Pointer multipartId, NativePartInfo.ByReference[] partInfoArray, IntBuffer nParts);
    int H3_CreatePart(Pointer handle, H3Auth token, Pointer multipartId, int partNumber, Pointer data, NativeLong size);
    int H3_CreatePartCopy(Pointer handle, H3Auth token, Pointer objectName, NativeLong offset, NativeLong size, Pointer multipartId, int partNumber);
}