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

    private String config = "config.ini";                           // Path of configuration
    private H3StoreType storeType = H3StoreType.H3_STORE_CONFIG;    // Use store that is specified in configuration
    private int userId = 0;                                         // Dummy userId
    private int MEGABYTE = 1048576;                                 // Size of a megabyte in bytes

     private String dir = "/tmp/h3";

    void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                if (!Files.isSymbolicLink(f.toPath())) {
                    deleteDir(f);
                }
            }
        }
        file.delete();
    }
    /**
     * Delete any files in /tmp/h3
     */
    @Before
    public void cleanup() {
        deleteDir(new File(dir));
    }


    /**
     * Create / Delete an object.
     */
    @Test
    public void simpleTest(){
        try {
            //Initialize client
            JH3 client = new JH3(storeType, config, userId);
            ArrayList<String> buckets;

            // Create some random data and store them into an H3Object
            byte[] data = new byte[3 * MEGABYTE];
            new Random().nextBytes(data);
            H3Object dataObj = new H3Object(data, 3* MEGABYTE);

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Create a bucket
            assertTrue(client.createBucket("b1"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Create an object in H3
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // List multiparts in bucket
            assertTrue(client.listMultiparts("b1").isEmpty());
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Create a multipart object
            H3MultipartId multipart = client.createMultipart("b1", "m1");
            assertNotNull(multipart);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Create a part
            assertTrue(client.createPart(dataObj, multipart, 1));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Create a second H3Object from a segment of the first
            H3Object dataObj2 = new H3Object(Arrays.copyOfRange(dataObj.getData(), 0, MEGABYTE), MEGABYTE);

            // Create a second part
            assertTrue(client.createPart(dataObj2, multipart, 0));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Create a third part
            assertTrue(client.createPart(dataObj, multipart, 2));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Create a part by copying
            assertTrue(client.createPartCopy("o1", 0, MEGABYTE, multipart, 0));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // List parts in multipart
            ArrayList<H3PartInfo> parts = client.listParts(multipart);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(parts);
            assertEquals(3, parts.size());

            for (H3PartInfo part :  parts){
                if (part.getPartNumber() == 0)
                    assertEquals(MEGABYTE, part.getSize());
                else
                    assertEquals(3 * MEGABYTE, part.getSize());
            }


            // Complete a multipart object
            assertTrue(client.completeMultipart(multipart));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Try to complete again
            assertFalse(client.completeMultipart(multipart));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Try to abort completed multipart
            assertFalse(client.abortMultipart(multipart));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Check if completed multipart appears as object in bucket
            assertTrue(client.listObjects("b1", 0).contains("m1"));

            // Check info of the object
            H3ObjectInfo objectInfo = client.infoObject("b1", "m1");
            assertNotNull(objectInfo);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertFalse(objectInfo.isCorrupt());
            assertEquals(7 * MEGABYTE, objectInfo.getSize());
            //assertEquals(0, objectInfo.getCreation());
            //assertEquals(0, objectInfo.getLastAccess());
            //assertEquals(0, objectInfo.getLastModification());

            // Delete objects
            assertTrue(client.deleteObject("b1", "m1"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // List objects
            assertTrue(client.listObjects("b1", 0).isEmpty());
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Delete bucket
            assertTrue(client.deleteBucket("b1"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // List buckets
            assertTrue(client.listBuckets().isEmpty());
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

        } catch (H3Exception e) {
            e.printStackTrace();
        }
    }
}
