package gr.forth.ics.JH3lib;

//import static org.junit.Assert.assertTrue;

import com.sun.jna.Memory;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;
import org.junit.Test;

import java.util.logging.Logger;

/**
 * Call each function from native h3lib
 */
public class LinkingTest
{
    private static Logger log = Logger.getLogger("IntegrationTest");
    @Test
    public void functionCalls(){
        try {
            // Get h3lib handle
            Pointer handle = JH3libInterface.INSTANCE.H3_Init(JH3libInterface.H3_StoreType.H3_STORE_REDIS, "~/.h3/config");

            // h3lib version
            String version = JH3libInterface.INSTANCE.H3_Version();
            log.info(version);

            // Dummy token
            H3Token myToken = new H3Token(17);

            H3BucketInfo bucketInfo = new H3BucketInfo();
            PointerByReference bucketNames = new PointerByReference();
            NativeLongByReference sizes = new NativeLongByReference();
            String data = "testValue";
            Pointer userData = new Memory(data.length() + 1);
            userData.setString(0, data);

            // Dummy Callback function
            JH3libInterface.h3_name_iterator_cb println = new JH3libInterface.h3_name_iterator_cb() {
                @Override
                public void apply(Pointer name, Pointer userData) {
                    System.out.println("name = " + name + ", userData = " + userData);
                }
            };
            // Bucket Operations
            log.info("CreateBucket: " + JH3libInterface.INSTANCE.H3_CreateBucket(handle, myToken, "bucket1"));
            log.info("ListBuckets: " + JH3libInterface.INSTANCE.H3_ListBuckets(handle, myToken, bucketNames, sizes));
            log.info("InfoBucket: " + JH3libInterface.INSTANCE.H3_InfoBucket(handle, myToken, "bucket1", bucketInfo));
            log.info("ForEachBucket: " + JH3libInterface.INSTANCE.H3_ForeachBucket(handle, myToken, println, userData));

            NativeLong size = new NativeLong(data.length() + 1);
            NativeLong offset = new NativeLong(0);
            PointerByReference objectNames = new PointerByReference();
            H3ObjectInfo objectInfo = new H3ObjectInfo();
            H3MultipartInfo multipartInfo = new H3MultipartInfo();
            PointerByReference id = new PointerByReference();
            PointerByReference partNumbers = new PointerByReference();

            // Object Operations
            log.info("CreateObject: " + JH3libInterface.INSTANCE.H3_CreateObject(handle, myToken, "bucket1", "object1", userData, size, offset));
            log.info("ListObjects: " + JH3libInterface.INSTANCE.H3_ListObjects(handle, myToken, "bucket1", "", offset, objectNames, sizes));
            log.info("InfoObject: " + JH3libInterface.INSTANCE.H3_InfoObject(handle, myToken, "bucket1", "object1", objectInfo));
            log.info("ForEachObject: " + JH3libInterface.INSTANCE.H3_ForeachObject(handle, myToken, "bucket1", "", size, 0L, println, userData));
            log.info("ReadObject: " + JH3libInterface.INSTANCE.H3_ReadObject(handle, myToken, "bucket1", "object1", offset, userData, sizes));
            log.info("WriteObject: " + JH3libInterface.INSTANCE.H3_WriteObject(handle, myToken, "bucket1", "object1", 0L, userData, size));
            log.info("CopyObject: " + JH3libInterface.INSTANCE.H3_CopyObject(handle, myToken, "bucket1", "object1", "object2"));
            log.info("CopyObjectRange:" + JH3libInterface.INSTANCE.H3_CopyObjectRange(handle, myToken, "bucket1", "object1", 0L, size, "object3"));
            log.info("MoveObject: " + JH3libInterface.INSTANCE.H3_MoveObject(handle, myToken, "bucket1", "object1", "object4"));

            // Multipart Operations
            log.info("CreateMultipart: " + JH3libInterface.INSTANCE.H3_CreateMultipart(handle, myToken, "bucket1", "object5", id));
            log.info("AbortMultipart: " + JH3libInterface.INSTANCE.H3_AbortMultipart(handle, myToken, id.getPointer()));
            log.info("CreateMultipart: " + JH3libInterface.INSTANCE.H3_CreateMultipart(handle, myToken, "bucket1", "object5", id));
            log.info("CompleteMultipart: " + JH3libInterface.INSTANCE.H3_CompleteMultipart(handle, myToken, id.getPointer()));
            log.info("ListMultiparts: " + JH3libInterface.INSTANCE.H3_ListMultiparts(handle, myToken, "bucket1", "", size, 0L, partNumbers, sizes));
            log.info("ListParts: " + JH3libInterface.INSTANCE.H3_ListParts(handle, myToken, id.getPointer(), size, 0L, multipartInfo, sizes));
            log.info("UploadPart: " + JH3libInterface.INSTANCE.H3_UploadPart(handle, myToken, id.getPointer(), 0, userData, size));
            log.info("UploadPartCopy: " + JH3libInterface.INSTANCE.H3_UploadPartCopy(handle, myToken, "object1", 0L, size, id.getPointer(), 1));

            // Delete Object
            log.info("DeleteObject:" + JH3libInterface.INSTANCE.H3_DeleteObject(handle, myToken, "bucket1", "object1"));
            // Delete Bucket
            log.info("DeleteBucket: " + JH3libInterface.INSTANCE.H3_DeleteBucket(handle, myToken, "bucket1"));
            // Free h3lib handle
            JH3libInterface.INSTANCE.H3_Free(handle);

            // Object Methods

        } catch (Exception e){
            System.out.println("e = " + e);
        }
    }
}
