package gr.forth.ics.JH3lib;

//import static org.junit.Assert.assertTrue;

import com.sun.jna.Memory;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.ByReference;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * Call each function from native h3lib
 */
public class LinkingTest
{
    private static Logger log = Logger.getLogger(LinkingTest.class.getName());
    private static Charset charset = StandardCharsets.UTF_8;      // Used to encode/decode Strings to/from ByteBuffer
    @Test
    public void functionCalls(){

        try {
            // Get h3lib handle
            ByteBuffer cfgFileName = ByteBuffer.wrap("~/.h3/config".getBytes(charset));
            Pointer handle = JH3libInterface.INSTANCE.H3_Init(JH3libInterface.H3StoreType.H3_STORE_FILESYSTEM, cfgFileName);

            // h3lib version
            Pointer version = JH3libInterface.INSTANCE.H3_Version();
            log.info(version.toString());

            // Dummy values
            H3Auth myToken = new H3Auth(17);
            H3BucketInfo bucketInfo = new H3BucketInfo();
            ByteBuffer bucketName = ByteBuffer.wrap("bucket1".getBytes(charset));
            ByteBuffer objectName = ByteBuffer.wrap("object1".getBytes(charset));
            ByteBuffer objectName2 = ByteBuffer.wrap("object2".getBytes(charset));
            ByteBuffer objectName3 = ByteBuffer.wrap("object3".getBytes(charset));
            ByteBuffer objectName4 = ByteBuffer.wrap("object4".getBytes(charset));
            ByteBuffer objectName5 = ByteBuffer.wrap("object5".getBytes(charset));
            ByteBuffer prefix = ByteBuffer.wrap("".getBytes(charset));
            PointerByReference bucketNameArray = new PointerByReference();
            IntBuffer nBuckets = IntBuffer.allocate(1);

            NativeLongByReference sizes = new NativeLongByReference();
            String data = "testValue";
            Pointer userData = new Memory(data.length() + 1);
            userData.setString(0, data);
            NativeLong size = new NativeLong(data.length() + 1);
            NativeLong offset = new NativeLong(0);
            PointerByReference objectNameArray = new PointerByReference();
            IntBuffer nObjects = IntBuffer.allocate(1);
            H3ObjectInfo objectInfo = new H3ObjectInfo();
            PointerByReference readData = new PointerByReference();
            PointerByReference partInfoArray = new PointerByReference();
            PointerByReference multipartId = new PointerByReference();
            PointerByReference multipartIdArray = new PointerByReference();
            IntByReference nParts = new IntByReference();
            IntBuffer  nIds = IntBuffer.allocate(1);

            // Dummy Callback function
            JH3libInterface.h3_name_iterator_cb println = new JH3libInterface.h3_name_iterator_cb() {
                @Override
                public void apply(Pointer name, Pointer userData) {
                    System.out.println("name = " + name + ", userData = " + userData);
                }
            };
            // Bucket Operations

            log.info("CreateBucket: " + JH3libInterface.INSTANCE.H3_CreateBucket(handle, myToken, bucketName));
            log.info("ListBuckets: " + JH3libInterface.INSTANCE.H3_ListBuckets(handle, myToken, bucketNameArray, nBuckets));
            log.info("InfoBucket: " + JH3libInterface.INSTANCE.H3_InfoBucket(handle, myToken, bucketName, bucketInfo, (byte) 1));
            log.info("ForEachBucket: " + JH3libInterface.INSTANCE.H3_ForeachBucket(handle, myToken, println, userData));


            // Object Operations
            log.info("CreateObject: " + JH3libInterface.INSTANCE.H3_CreateObject(handle, myToken, bucketName, objectName, userData, size));
            log.info("CreateObjectCopy: " + JH3libInterface.INSTANCE.H3_CreateObjectCopy(handle, myToken, bucketName, objectName, offset, size, objectName2));
            log.info("ListObjects: " + JH3libInterface.INSTANCE.H3_ListObjects(handle, myToken, bucketName, prefix,0, objectNameArray, nObjects));
            log.info("InfoObject: " + JH3libInterface.INSTANCE.H3_InfoObject(handle, myToken, bucketName , objectName, objectInfo));
            log.info("ForEachObject: " + JH3libInterface.INSTANCE.H3_ForeachObject(handle, myToken, bucketName, prefix, 2, 0, println, userData));
            log.info("ReadObject: " + JH3libInterface.INSTANCE.H3_ReadObject(handle, myToken, bucketName, objectName2, offset, readData, sizes));
            log.info("DeleteObject:" + JH3libInterface.INSTANCE.H3_DeleteObject(handle, myToken, bucketName, objectName2));
            log.info("WriteObject: " + JH3libInterface.INSTANCE.H3_WriteObject(handle, myToken, bucketName, objectName2, userData, size, offset));
            log.info("DeleteObject:" + JH3libInterface.INSTANCE.H3_DeleteObject(handle, myToken, bucketName, objectName2));
            log.info("WriteObjectCopy: " + JH3libInterface.INSTANCE.H3_WriteObjectCopy(handle, myToken, bucketName, objectName, offset, size, objectName2, offset));
            log.info("CopyObject: " + JH3libInterface.INSTANCE.H3_CopyObject(handle, myToken, bucketName, objectName, objectName3, (byte) 0));
            log.info("MoveObject: " + JH3libInterface.INSTANCE.H3_MoveObject(handle, myToken, bucketName, objectName, objectName4, (byte) 0));

            // TODO try multipartId.getPointer instead of creating new ByteBuffer
            // Multipart Operations
            log.info("CreateMultipart: " + JH3libInterface.INSTANCE.H3_CreateMultipart(handle, myToken, bucketName, objectName, multipartId));
            ByteBuffer multipartIdString = ByteBuffer.wrap(multipartId.getValue().getString(0).getBytes(charset));
            log.info("AbortMultipart: " + JH3libInterface.INSTANCE.H3_AbortMultipart(handle, myToken, multipartIdString));
            log.info("CreateMultipart: " + JH3libInterface.INSTANCE.H3_CreateMultipart(handle, myToken, bucketName, objectName5, multipartId));
            multipartIdString = ByteBuffer.wrap(multipartId.getValue().getString(0).getBytes(charset));
            log.info("CreatePart: " + JH3libInterface.INSTANCE.H3_CreatePart(handle, myToken, multipartIdString, 0, userData, size));
            log.info("CreatePartCopy: " + JH3libInterface.INSTANCE.H3_CreatePartCopy(handle, myToken, objectName, offset, size, multipartIdString, 1));
            log.info("ListParts: " + JH3libInterface.INSTANCE.H3_ListParts(handle, myToken, multipartIdString, partInfoArray, nParts));
            log.info("ListMultiparts: " + JH3libInterface.INSTANCE.H3_ListMultiparts(handle, myToken, bucketName, 0, multipartIdArray, nIds));
            log.info("CompleteMultipart: " + JH3libInterface.INSTANCE.H3_CompleteMultipart(handle, myToken, multipartIdString));

            // Delete Bucket
            log.info("DeleteBucket: " + JH3libInterface.INSTANCE.H3_DeleteBucket(handle, myToken, bucketName));
            // Free h3lib handle
            JH3libInterface.INSTANCE.H3_Free(handle);

            // Object Methods

        } catch (Exception e){
            System.out.println("e = " + e);
        }
    }
}
