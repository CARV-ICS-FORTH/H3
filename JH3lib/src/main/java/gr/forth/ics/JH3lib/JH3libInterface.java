package gr.forth.ics.JH3lib;

import com.sun.jna.*;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;


// TODO Consider using String instead of String for char *
/**
 * Interface that maps H3lib's native functions to Java
 */
public interface JH3libInterface extends Library {
   // public static final String JNA_LIBRARY_NAME = "/usr/local/lib/libH3.so";
   // public static final NativeLibrary JNA_NATIVE_LIB = NativeLibrary.getInstance(JH3libInterface.JNA_LIBRARY_NAME);
   // public static final JH3libInterface INSTANCE = (JH3libInterface) Native.load(JH3libInterface.JNA_LIBRARY_NAME, JH3libInterface.class);

    // Might just use this
    public static final JH3libInterface INSTANCE = (JH3libInterface) Native.load("h3lib", JH3libInterface.class);

    // Defines of h3lib
    public static final int H3_SUCCESS = (int)1;
    public static final int H3_FAILURE = (int)0;
    public static final int H3_ERROR_MESSAGE = (int)512;
    public static final int H3_BUCKET_NAME_SIZE = (int)64;
    public static final int H3_OBJECT_NAME_SIZE = (int)512;

    // enum of StoreTypes
    public static interface H3_StoreType {
        public static final int H3_STORE_REDIS = 0;
        public static final int H3_STORE_ROCKSDB = 1;
        public static final int H3_STORE_KREON = 2;
        public static final int H3_STORE_FILESYSTEM = 3;
        public static final int H3_STORE_IME = 4;
        public static final int H3_STORE_NumOfStores = 5;
    };

    // callback function
    public interface h3_name_iterator_cb extends Callback {
        void apply(Pointer name, Pointer userData);
    }

    // Version of H3
    String H3_Version();


    // Handle Management
    Pointer H3_Init(int storageType, String cfgFileName);
    void H3_Free(Pointer handle);

    // Bucket Management
    int H3_CreateBucket(Pointer handle, H3Token token, String bucketName);
    int H3_DeleteBucket(Pointer handle, H3Token token, String bucketName);
    int H3_ListBuckets(Pointer handle, H3Token token, PointerByReference bucketNames, NativeLongByReference size);
    int H3_ForeachBucket(Pointer handle, H3Token token, JH3libInterface.h3_name_iterator_cb function, Pointer userData);
    int H3_InfoBucket(Pointer handle, H3Token token,  String bucketName, H3BucketInfo bucketInfo);

    // Object Management
    int H3_CreateObject(Pointer handle, H3Token token, String bucketName, String objectName, Pointer data, NativeLong size, NativeLong offset);
    int H3_DeleteObject(Pointer handle, H3Token token, String bucketName, String objectName);
    int H3_ListObjects(Pointer handle, H3Token token, String bucketName, String prefix, NativeLong offset, PointerByReference objectNames, NativeLongByReference size);
    int H3_ForeachObject(Pointer handle, H3Token token, String bucketName, String prefix, NativeLong maxSize, long offset, JH3libInterface.h3_name_iterator_cb function, Pointer userData);
    int H3_InfoObject(Pointer handle, H3Token token, String bucketName, String objectName, H3ObjectInfo objectInf);
    int H3_ReadObject(Pointer handle, H3Token token, String bucketName, String objectName, NativeLong offset, Pointer data, NativeLongByReference size);
    int H3_WriteObject(Pointer handle, H3Token token, String bucketName, String objectName, long offset, Pointer data, NativeLong size);
    int H3_CopyObject(Pointer handle, H3Token token, String bucketName, String srcObjectName, String dstObjectName);
    int H3_CopyObjectRange(Pointer handle, H3Token token, String bucketName, String srcObjectName, long offset, NativeLong size, String dstObjectName);
    int H3_MoveObject(Pointer handle, H3Token token, String bucketName, String srcObjectName, String dstObjectName);

    // Multipart Management
    int H3_CreateMultipart(Pointer handle, H3Token token, String bucketName, String objectName, PointerByReference id);
    int H3_CompleteMultipart(Pointer handle, H3Token token, Pointer id);
    int H3_AbortMultipart(Pointer handle, H3Token token, Pointer id);
    int H3_ListMultiparts(Pointer handle, H3Token token, String bucketName, String prefix, NativeLong maxSize, long offset, PointerByReference idArray, NativeLongByReference size);
    int H3_ListParts(Pointer handle, H3Token token, Pointer id, NativeLong maxSize, long offset, H3MultipartInfo info, NativeLongByReference size);
    int H3_UploadPart(Pointer handle, H3Token token, Pointer id, int partNumber, Pointer data, NativeLong size);
    int H3_UploadPartCopy(Pointer handle, H3Token token, String objectName, long offset, NativeLong size, Pointer id, int partNumber);
}