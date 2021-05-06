package gr.forth.ics.JH3lib;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class JH3MultipartTest {

    // Initialize storage URI; if H3LIB_STORAGE_URI is not set, use default uri (local filesystem: /tmp/h3) 
    private static String storageURI = null;
    static {   
      storageURI = System.getenv("H3LIB_STORAGE_URI");
      if(storageURI == null){
        storageURI = "file:///tmp/h3";
      }
    }   

    private int userId = 0;                                         // Dummy userId
    private int MEGABYTE = 1048576;                                 // Size of a megabyte in bytes


    /** 
     * Cleanup backend before each test.
     */
    @Before
    public void cleanup(){
      try{
        JH3 client = new JH3(storageURI, userId);
        ArrayList<String> buckets = client.listBuckets();
        for (String bucket : buckets){
          ArrayList<String> objects = client.listObjects(bucket, 0);
          for (String object : objects){
            client.deleteObject(bucket, object);
          }   
          client.deleteBucket(bucket);
        }  
      }catch(JH3Exception e){}
    }   

    /**
     * Create / Delete an object.
     */
    @Test
    public void simpleTest(){
        try {
            //Initialize client
            JH3 client = new JH3(storageURI, userId);
            ArrayList<String> buckets;

            // Create some random data and store them into an H3Object
            byte[] data = new byte[3 * MEGABYTE];
            new Random().nextBytes(data);
            JH3Object dataObj = new JH3Object(data, 3* MEGABYTE);

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Create a bucket
            assertTrue(client.createBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create an object in H3
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // List multiparts in bucket
            assertTrue(client.listMultiparts("b1").isEmpty());
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create a multipart object
            JH3MultipartId multipart = client.createMultipart("b1", "m1");
            assertNotNull(multipart);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create a part
            assertTrue(client.createPart(dataObj, multipart, 1));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create a second H3Object from a segment of the first
            JH3Object dataObj2 = new JH3Object(Arrays.copyOfRange(dataObj.getData(), 0, MEGABYTE), MEGABYTE);

            // Create a second part
            assertTrue(client.createPart(dataObj2, multipart, 0));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create a third part
            assertTrue(client.createPart(dataObj, multipart, 2));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create a part by copying
            assertTrue(client.createPartCopy("o1", 0, MEGABYTE, multipart, 0));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // List parts in multipart
            ArrayList<JH3PartInfo> parts = client.listParts(multipart);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(parts);
            assertEquals(3, parts.size());

            for (JH3PartInfo part :  parts){
                if (part.getPartNumber() == 0)
                    assertEquals(MEGABYTE, part.getSize());
                else
                    assertEquals(3 * MEGABYTE, part.getSize());
            }


            // Complete a multipart object
            assertTrue(client.completeMultipart(multipart));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Try to complete again
            assertFalse(client.completeMultipart(multipart));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Try to abort completed multipart
            assertFalse(client.abortMultipart(multipart));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Check if completed multipart appears as object in bucket
            assertTrue(client.listObjects("b1", 0).contains("m1"));

            // Check info of the object
            JH3ObjectInfo objectInfo = client.infoObject("b1", "m1");
            assertNotNull(objectInfo);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertFalse(objectInfo.isCorrupt());
            assertEquals(7 * MEGABYTE, objectInfo.getSize());
            //assertEquals(0, objectInfo.getCreation());
            //assertEquals(0, objectInfo.getLastAccess());
            //assertEquals(0, objectInfo.getLastModification());

            // Delete objects
            assertTrue(client.deleteObject("b1", "m1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // List objects
            assertTrue(client.listObjects("b1", 0).isEmpty());
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete bucket
            assertTrue(client.deleteBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // List buckets
            assertTrue(client.listBuckets().isEmpty());
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

        } catch (JH3Exception e) {
            e.printStackTrace();
        }
    }
}
