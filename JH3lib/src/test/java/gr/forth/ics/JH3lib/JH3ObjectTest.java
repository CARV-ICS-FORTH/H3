package gr.forth.ics.JH3lib;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.*;

public class JH3ObjectTest {
    private String config = "config.ini";                                   // Path of configuration file
    private H3StoreType storeType = H3StoreType.H3_STORE_CONFIG;            // Use local filesystem
    private int userId = 0;                                                 // Dummy userId
    private int MEGABYTE = 1048576;
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
     * Create / Delete an object
     */
    @Test
    public void simpleTest() {

        try {
            ArrayList<String> buckets;
            ArrayList<String> objects;

            // Initialize client
            JH3Client client = new JH3Client(storeType, config, userId);

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Get info of non-existent object from non-existent bucket
            assertNull(client.infoObject("b1", "o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Delete non-existent object from non-existent bucket
            assertFalse(client.deleteObject("b1", "o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Read non-existent object from non-existent bucket
            assertNull(client.readObject("b1", "o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Create a bucket
            assertTrue(client.createBucket("b1"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Get info of non-existent object
            assertNull(client.infoObject("b1","o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Delete non-existent object
            assertFalse(client.deleteObject("b1","o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Read non-existent object
            assertNull(client.readObject("b1","o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Create some random data and store them into an H3Object
            byte[] data = new byte[3 * MEGABYTE];
            new Random().nextBytes(data);
            H3Object dataObj = new H3Object(data, 3* MEGABYTE);

            // Write the first object
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Write on existent object
            assertFalse(client.createObject("b1","o1", dataObj));
            assertEquals(H3Status.H3_EXISTS, client.getStatus());

            // Get info of object
            H3ObjectInfo objectInfo = client.infoObject("b1", "o1");
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3* MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read object
            H3Object readObj  = client.readObject("b1", "o1");
            assertNotNull(readObj);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertTrue(Arrays.equals(data, readObj.getData()));

            // Read part of object without offset
            readObj = client.readObject("b1", "o1", 0, MEGABYTE);
            assertEquals(H3Status.H3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 0, MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // Read part of object with offsets
            readObj = client.readObject("b1", "o1", MEGABYTE, MEGABYTE);
            assertEquals(H3Status.H3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, MEGABYTE, 2*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            readObj = client.readObject("b1", "o1", 2*MEGABYTE, MEGABYTE);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 2 * MEGABYTE, 3*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // List objects
            ArrayList<String> expected = new ArrayList<>();
            expected.add("o1");
            objects = client.listObjects("b1", 0);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);

            // Write second object
            assertTrue(client.writeObject("b1", "o2", dataObj));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Get info of second object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3*MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read full object
            readObj = client.readObject("b1", "o2");
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(data, readObj.getData()));
            assertEquals(3* MEGABYTE, readObj.getSize());


            // List objects
            expected.add("o2");
            objects = client.listObjects("b1", 0);
            Collections.sort(objects);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertEquals(expected, objects);

            // Overwrite second object
            assertTrue(client.writeObject("b1", "o2", dataObj));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Get info of object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3*MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read part of object without offset
            readObj = client.readObject("b1", "o2", 0, MEGABYTE);
            assertEquals(H3Status.H3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 0, MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // Read part of object with offsets
            readObj = client.readObject("b1", "o2", MEGABYTE, MEGABYTE);
            assertEquals(H3Status.H3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, MEGABYTE, 2*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            readObj = client.readObject("b1", "o2", 2*MEGABYTE, MEGABYTE);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 2 * MEGABYTE, 3*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // List objects
            objects = client.listObjects("b1", 0);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);

            // Check bucket info
            H3BucketInfo bucketInfo = client.infoBucket("b1");
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());


            // Delete first object
            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Delete first object again
            assertFalse(client.deleteObject("b1", "o1"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            expected.remove("o1");

            // List objects
            objects = client.listObjects("b1", 0);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);


            // Write third object
            assertTrue(client.writeObject("b1", "o3", dataObj));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Move second object to third; don't overwrite
            assertFalse(client.moveObject("b1", "o2", "o3", true));
            assertEquals(H3Status.H3_EXISTS, client.getStatus());

            // Make sure third object still exists
            assertTrue(client.listObjects("b1", 0).contains("o3"));

            // Move second object to third
            assertTrue(client.moveObject("b1", "o2", "o3"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Try to move non-existent object
            assertFalse(client.moveObject("b1", "o2", "o3"));
            assertEquals(H3Status.H3_NOT_EXISTS, client.getStatus());

            // Copy third object back to second
            assertTrue(client.copyObject("b1", "o3", "o2"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Copy again, allowing overwrites
            assertTrue(client.copyObject("b1", "o3", "o2"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Copy again, not allowing overwrites
            assertFalse(client.copyObject("b1", "o3", "o2", true));
            assertEquals(H3Status.H3_FAILURE, client.getStatus());

            // Get info of object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3*MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read part of object without offset
            readObj = client.readObject("b1", "o2", 0, MEGABYTE);
            assertEquals(H3Status.H3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 0, MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // Read part of object with offsets
            readObj = client.readObject("b1", "o2", MEGABYTE, MEGABYTE);
            assertEquals(H3Status.H3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, MEGABYTE, 2*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            readObj = client.readObject("b1", "o2", 2*MEGABYTE, MEGABYTE);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 2 * MEGABYTE, 3*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            expected.add("o3");
            // List Objects
            objects = client.listObjects("b1", 0);
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);

            // Delete third object
            assertTrue(client.deleteObject("b1", "o3"));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            expected.remove("o3");
            assertEquals(expected, client.listObjects("b1", 0));
            assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Delete second object
            assertTrue(client.deleteObject("b1", "o2"));

            expected.remove("o2");
            assertEquals(expected, client.listObjects("b1",0));
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
