package gr.forth.ics.JH3lib;

import com.sun.jna.Memory;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.NativeLongByReference;
import com.sun.jna.ptr.PointerByReference;
import org.junit.Ignore;

import java.io.File;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Call each function from native h3lib.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class LinkingTest
{
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_RESET = "\u001B[0m";

    private static Logger log = null; //Logger.getLogger(LinkingTest.class.getName());
    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[" + ANSI_GREEN + "H3%4$s" + ANSI_RESET + "] %5$s %n");
        log = Logger.getLogger(LinkingTest.class.getName());
    }


    // Initialize storage URI; if H3LIB_STORAGE_URI is not set, use default uri (local filesystem: /tmp/h3) 
    private static String storageURI = null;
    static {
      storageURI = System.getenv("H3LIB_STORAGE_URI");
      if(storageURI == null){
          storageURI = "file:///tmp/h3";
      }
    }

    private NativeAuth myToken = new NativeAuth(17);        // Dummy authorization
    
    // Dummy callback function
    private JH3Interface.h3_name_iterator_cb println = new JH3Interface.h3_name_iterator_cb() {
        @Override
        public void apply(Pointer name, Pointer userData) {
            System.out.println("name = " + name.getString(0) + ", userData = " + userData.getString(0));
        }
    };

    /**
     * Print current version of h3lib
     */
    @Ignore
    public void printVersion(){
        log.info("---- printVersion ----");

        // h3lib version
        Pointer version = JH3Interface.INSTANCE.H3_Version();
        log.info("h3lib Version: " + version.getString(0));
    }

    /**
     * Basic test of h3lib handle.
     */
    @Ignore
    public void testHandle(){
        log.info("---- testHandle ----");

        // Map configuration path to pointer
        Pointer uri = new Memory(storageURI.length() + 1);
        uri.setString(0, storageURI);
        log.info("Using Storage URI: " + uri.getString(0));

        // Get h3lib handle
        Pointer handle = JH3Interface.INSTANCE.H3_Init(uri);
        assertNotEquals(null, handle);
        log.info("Initialized h3lib handle successfully");

        // Call Free
        JH3Interface.INSTANCE.H3_Free(handle);
    }

    /**
     * Test basic create/delete/list bucket operations.
     */
    @Ignore
    public void basicBucketCalls() {
        log.info("---- basicBucketCalls ----");
        try {
            // Get h3lib handle
            Pointer uri = new Memory(storageURI.length() + 1);
            uri.setString(0, storageURI);
            Pointer handle = JH3Interface.INSTANCE.H3_Init(uri);
            assertNotEquals(null, handle);

            // Create a bucket
            String b1 = "bucket1";
            Pointer bucketName = new Memory(b1.length() + 1);
            bucketName.setString(0, b1);
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_CreateBucket(handle, myToken, bucketName));

            // Create a second bucket
            String b2 = "bucket0";
            Pointer bucketName2 = new Memory(b2.length() + 1);
            bucketName2.setString(0, b2);
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_CreateBucket(handle, myToken, bucketName2));

            log.info("Created buckets: {" + b1 + ", " + b2 + "} successfully");

            // List buckets
            PointerByReference bucketNameArray = new PointerByReference();
            IntBuffer nBuckets = IntBuffer.allocate(1);
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_ListBuckets(handle, myToken, bucketNameArray, nBuckets));
            assertEquals(2, nBuckets.get(0));

            ArrayList<String> buckets = new ArrayList<>();
            for (int i = 0, arrayOffset = 0; i < nBuckets.get(0); i++) {
                String tmp = bucketNameArray.getValue().getString(arrayOffset);
                buckets.add(tmp);
                arrayOffset += tmp.length();
                while (bucketNameArray.getValue().getByte(arrayOffset) == '\0')
                    arrayOffset++;
            }
            ArrayList<String> expected = new ArrayList<>();
            expected.add(b1);
            expected.add(b2);

            // Check if the result is correct
            assertEquals(expected, buckets);
            log.info("List buckets result: " + buckets.toString());

            // Delete Buckets
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_DeleteBucket(handle, myToken, bucketName));
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_DeleteBucket(handle, myToken, bucketName2));

            // Check if list is empty
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_ListBuckets(handle,myToken, bucketNameArray, nBuckets));
            assertEquals(0, nBuckets.get(0));
            log.info("Buckets were deleted successfully");

            JH3Interface.INSTANCE.H3_Free(handle);

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("e = " + e);
        }
    }

    /**
     * Test basic create/list/delete object operations.
     */
    @Ignore
    public void basicObjectCalls(){
        log.info("---- basicObjectCalls ----");
        try{
            // Get h3lib handle
            Pointer uri = new Memory(storageURI.length() + 1);
            uri.setString(0, storageURI);
            Pointer handle = JH3Interface.INSTANCE.H3_Init(uri);
            assertNotEquals(null, handle);

            String b1 = "bucket1";
            Pointer bucketName = new Memory(b1.length() + 1);
            bucketName.setString(0, b1);
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_CreateBucket(handle, myToken, bucketName));

            // Create dummy data
            String data = "dummyValue";
            Pointer userData = new Memory(data.length() + 1);
            userData.setString(0, data);
            NativeLong size = new NativeLong(data.length() + 1);

            // Create first object
            String object1 = "object1";
            Pointer objectName = new Memory(object1.length() +1);
            objectName.setString(0, object1);
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_CreateObject(handle, myToken, bucketName, objectName, userData, size));

            // List current objects
            PointerByReference objectNameArray = new PointerByReference();
            IntBuffer nObjects = IntBuffer.allocate(1);
            Pointer prefix = new Memory("".length() +1);
            objectName.setString(0, "");

            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_ListObjects(handle, myToken, bucketName, prefix,0, objectNameArray, nObjects));
            assertEquals(1, nObjects.get(0));

            // Create second object by copying first
            String object2 = "object2";
            Pointer objectName2 = new Memory(object2.length() +1);
            objectName.setString(0, object2);
            NativeLong offset = new NativeLong(0);
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_CreateObjectCopy(handle, myToken, bucketName, objectName, offset, new NativeLongByReference(size), objectName2));



            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_ListObjects(handle, myToken, bucketName, prefix,0, objectNameArray, nObjects));
            assertEquals(2, nObjects.get(0));

            ArrayList<String> objects = new ArrayList<>();
            for (int i = 0, arrayOffset = 0; i < nObjects.get(0); i++) {
                String tmp = objectNameArray.getValue().getString(arrayOffset);
                objects.add(tmp);
                arrayOffset += tmp.length();
                while (objectNameArray.getValue().getByte(arrayOffset) == '\0')
                    arrayOffset++;
            }
            ArrayList<String> expected = new ArrayList<>();
            expected.add(object1);
            expected.add(object2);

            // Check if result of list is correct
            assertEquals(expected, objects);
            log.info("List objects result: " + objects.toString());

            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_DeleteObject(handle, myToken, bucketName, objectName));
            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_DeleteObject(handle, myToken, bucketName, objectName2));

            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_ListObjects(handle, myToken, bucketName, prefix,0, objectNameArray, nObjects));
            assertEquals(0, nObjects.get(0));
            log.info("Objects were deleted successfully");

            assertEquals(JH3Interface.Status.H3_SUCCESS, JH3Interface.INSTANCE.H3_DeleteBucket(handle, myToken, bucketName));

            // Free h3lib handle
            JH3Interface.INSTANCE.H3_Free(handle);


           // Pointer objectName3 = new Memory("object3".length() +1);
           // objectName.setString(0, "object3");
           // Pointer objectName4 = new Memory("object4".length() +1);
           // objectName.setString(0, "object4");
           // Pointer objectName5 = new Memory("object5".length() +1);
           // objectName.setString(0, "object5");


           // NativeLongByReference sizes = new NativeLongByReference();

           // H3ObjectInfo objectInfo = new H3ObjectInfo();
           // PointerByReference readData = new PointerByReference();
           // H3PartInfo.ByReference[] partInfoArray = null;
           // PointerByReference multipartId = new PointerByReference();
           // PointerByReference multipartIdArray = new PointerByReference();
           // IntBuffer nParts = IntBuffer.allocate(1);
           // IntBuffer  nIds = IntBuffer.allocate(1);



           // // Object Operations
           // log.info("InfoObject: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_InfoObject(handle, myToken, bucketName , objectName, objectInfo)).name());
           // log.info("ForEachObject: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_ForeachObject(handle, myToken, bucketName, prefix, 2, 0, println, userData)).name());
           // log.info("ReadObject: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_ReadObject(handle, myToken, bucketName, objectName2, offset, readData, sizes)).name());
           // log.info("WriteObject: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_WriteObject(handle, myToken, bucketName, objectName2, userData, size, offset)).name());
           // log.info("DeleteObject:" + H3Status.fromInt(JH3libInterface.INSTANCE.H3_DeleteObject(handle, myToken, bucketName, objectName2)).name());
           // log.info("WriteObjectCopy: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_WriteObjectCopy(handle, myToken, bucketName, objectName, offset, size, objectName2, offset)).name());
           // log.info("CopyObject: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_CopyObject(handle, myToken, bucketName, objectName, objectName3, (byte) 0)).name());
           // log.info("MoveObject: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_MoveObject(handle, myToken, bucketName, objectName, objectName4, (byte) 0)).name());

           // // TODO try multipartId.getPointer instead of creating new Pointer
           // // Multipart Operations
           // log.info("CreateMultipart: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreateMultipart(handle, myToken, bucketName, objectName, multipartId)).name());
           // log.info("multipartId: " + multipartId.getValue());

           // Pointer multipartIdString =  new Memory(multipartId.getValue().getString(0).length() + 1);
           // multipartIdString.setString(0, multipartId.getValue().getString(0));
           // log.info("AbortMultipart: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_AbortMultipart(handle, myToken, multipartIdString)).name());
           // log.info("CreateMultipart: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreateMultipart(handle, myToken, bucketName, objectName5, multipartId)).name());
           // multipartIdString =  new Memory(multipartId.getValue().getString(0).length() + 1);
           // multipartIdString.setString(0, multipartId.getValue().getString(0));
           // log.info("CreatePart: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreatePart(handle, myToken, multipartIdString, 0, userData, size)).name());
           // log.info("CreatePartCopy: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_CreatePartCopy(handle, myToken, objectName, offset, size, multipartIdString, 1)).name());
           // log.info("ListParts: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_ListParts(handle, myToken, multipartIdString, partInfoArray, nParts)).name());
           // for(int i = 0; i < nParts.get(0); i++)
           //     log.info(partInfoArray[i].toString());
           // log.info("ListMultiparts: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_ListMultiparts(handle, myToken, bucketName, 0, multipartIdArray, nIds)).name());
           // log.info("CompleteMultipart: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_CompleteMultipart(handle, myToken, multipartIdString)).name());


            // Object Methods

        } catch (Exception e){
            //e.printStackTrace();
            System.out.println("e = " + e);
        }
    }

    @Ignore
    public void advancedBucketCalls(){
        // Retrieve bucket info
        //H3BucketInfo bucketInfo = new H3BucketInfo();
        //log.info("InfoBucket(without stats): " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_InfoBucket(handle, myToken, bucketName, bucketInfo, (byte) 0)).name());
        //log.info(bucketInfo.toString());
        //log.info("InfoBucket(with stats): " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_InfoBucket(handle, myToken, bucketName, bucketInfo, (byte) 1)).name());
        //log.info(bucketInfo.toString());

        //// Test callback function on buckets
        //String data = "testValue";
        //Pointer userData = new Memory(data.length() + 1);
        //userData.setString(0, data);
        //log.info("ForEachBucket: " + H3Status.fromInt(JH3libInterface.INSTANCE.H3_ForeachBucket(handle, myToken, println, userData)).name());

    }
}
