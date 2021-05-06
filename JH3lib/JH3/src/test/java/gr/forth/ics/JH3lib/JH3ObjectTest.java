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
    
  
    // Initialize storage URI; if H3LIB_STORAGE_URI is not set, use default uri (local filesystem: /tmp/h3) 
    private static String storageURI = null;
    static {   
      storageURI = System.getenv("H3LIB_STORAGE_URI");
      if(storageURI == null){
        storageURI = "file:///tmp/h3";
      }
    }   
    
    private int userId = 0;                                                 // Dummy userId
    private int MEGABYTE = 1048576;


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
     * Create / Delete an object
     */
    @Test
    public void simpleTest() {

        try {
            ArrayList<String> buckets;
            ArrayList<String> objects;

            // Initialize client
            JH3 client = new JH3(storageURI, userId);

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Get info of non-existent object from non-existent bucket
            assertNull(client.infoObject("b1", "o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Delete non-existent object from non-existent bucket
            assertFalse(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Read non-existent object from non-existent bucket
            assertNull(client.readObject("b1", "o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Create a bucket
            assertTrue(client.createBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Get info of non-existent object
            assertNull(client.infoObject("b1","o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Delete non-existent object
            assertFalse(client.deleteObject("b1","o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Read non-existent object
            assertNull(client.readObject("b1","o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Create some random data and store them into an H3Object
            byte[] data = new byte[3 * MEGABYTE];
            new Random().nextBytes(data);
            JH3Object dataObj = new JH3Object(data, 3* MEGABYTE);
            
            // Large object name
            String largeName = new String(new char[JH3.H3_OBJECT_NAME_SIZE + 1]).replace("\0", "a");
            assertFalse(client.createObject("b1", largeName, dataObj));
            assertEquals(JH3Status.JH3_NAME_TOO_LONG, client.getStatus());

            // Write the first object
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Write on existent object
            assertFalse(client.createObject("b1","o1", dataObj));
            assertEquals(JH3Status.JH3_EXISTS, client.getStatus());

            // Get info of object
            JH3ObjectInfo objectInfo = client.infoObject("b1", "o1");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3* MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read object
            JH3Object readObj  = client.readObject("b1", "o1");
            assertNotNull(readObj);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(Arrays.equals(data, readObj.getData()));

            // Read part of object without offset
            readObj = client.readObject("b1", "o1", 0, MEGABYTE);
            assertEquals(JH3Status.JH3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 0, MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // Read part of object with offsets
            readObj = client.readObject("b1", "o1", MEGABYTE, MEGABYTE);
            assertEquals(JH3Status.JH3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, MEGABYTE, 2*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            readObj = client.readObject("b1", "o1", 2*MEGABYTE, MEGABYTE);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 2 * MEGABYTE, 3*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // List objects
            ArrayList<String> expected = new ArrayList<>();
            expected.add("o1");
            objects = client.listObjects("b1", 0);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);

            // Write second object
            assertTrue(client.writeObject("b1", "o2", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Get info of second object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3*MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read full object
            readObj = client.readObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(data, readObj.getData()));
            assertEquals(3* MEGABYTE, readObj.getSize());


            // List objects
            expected.add("o2");
            objects = client.listObjects("b1", 0);
            Collections.sort(objects);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertEquals(expected, objects);

            // Overwrite second object
            assertTrue(client.writeObject("b1", "o2", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Get info of object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3*MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read part of object without offset
            readObj = client.readObject("b1", "o2", 0, MEGABYTE);
            assertEquals(JH3Status.JH3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 0, MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // Read part of object with offsets
            readObj = client.readObject("b1", "o2", MEGABYTE, MEGABYTE);
            assertEquals(JH3Status.JH3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, MEGABYTE, 2*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            readObj = client.readObject("b1", "o2", 2*MEGABYTE, MEGABYTE);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 2 * MEGABYTE, 3*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // List objects
            objects = client.listObjects("b1", 0);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);

            // Check bucket info
            JH3BucketInfo bucketInfo = client.infoBucket("b1");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());


            // Delete first object
            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete first object again
            assertFalse(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            expected.remove("o1");

            // List objects
            objects = client.listObjects("b1", 0);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);


            // Write third object
            assertTrue(client.writeObject("b1", "o3", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Move second object to third; don't overwrite
            assertFalse(client.moveObject("b1", "o2", "o3", true));
            assertEquals(JH3Status.JH3_EXISTS, client.getStatus());

            // Make sure third object still exists
            assertTrue(client.listObjects("b1", 0).contains("o3"));

            // Move second object to third
            assertTrue(client.moveObject("b1", "o2", "o3"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Try to move non-existent object
            assertFalse(client.moveObject("b1", "o2", "o3"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Copy third object back to second
            assertTrue(client.copyObject("b1", "o3", "o2"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Copy again, allowing overwrites
            assertTrue(client.copyObject("b1", "o3", "o2"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Copy again, not allowing overwrites
            assertFalse(client.copyObject("b1", "o3", "o2", true));
            assertEquals(JH3Status.JH3_FAILURE, client.getStatus());

            // Get info of object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3*MEGABYTE, objectInfo.getSize());
            //assertNotEquals(0, objectInfo.getCreation());
            //assertNotEquals(0, objectInfo.getLastAccess());
            //assertNotEquals(0, objectInfo.getLastModification());

            // Read part of object without offset
            readObj = client.readObject("b1", "o2", 0, MEGABYTE);
            assertEquals(JH3Status.JH3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 0, MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            // Read part of object with offsets
            readObj = client.readObject("b1", "o2", MEGABYTE, MEGABYTE);
            assertEquals(JH3Status.JH3_CONTINUE, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, MEGABYTE, 2*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            readObj = client.readObject("b1", "o2", 2*MEGABYTE, MEGABYTE);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(Arrays.copyOfRange(data, 2 * MEGABYTE, 3*MEGABYTE), readObj.getData()));
            assertEquals(MEGABYTE, readObj.getSize());

            expected.add("o3");
            // List Objects
            objects = client.listObjects("b1", 0);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            Collections.sort(objects);
            assertEquals(expected, objects);

            // Delete third object
            assertTrue(client.deleteObject("b1", "o3"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            expected.remove("o3");
            assertEquals(expected, client.listObjects("b1", 0));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete second object
            assertTrue(client.deleteObject("b1", "o2"));

            expected.remove("o2");
            assertEquals(expected, client.listObjects("b1",0));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create an object again
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Truncate object; reduce size to 1MB
            //assertTrue(client.truncateObject("b1", "o1", MEGABYTE));
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Get info of object
            //objectInfo = client.infoObject("b1", "o1");
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            //assertNotNull(objectInfo);
            //assertEquals(MEGABYTE, objectInfo.getSize());

            // Truncate object; extend size to 10MB
            //assertTrue(client.truncateObject("b1", "o1", 10 * MEGABYTE));
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Get info
            //objectInfo = client.infoObject("b1", "o1");
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            //assertNotNull(objectInfo);
            //assertEquals(10 * MEGABYTE, objectInfo.getSize());

            // truncate object; reduce size to 0
            //assertTrue(client.truncateObject("b1", "o1"));
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // Get info
            //objectInfo = client.infoObject("b1", "o1");
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            //assertNotNull(objectInfo);
            //assertEquals(0, objectInfo.getSize());

            // Delete object
            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Exchange non-existent objects
            assertFalse(client.exchangeObject("b1", "o1", "o2"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Create first objects to be exchanged
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Exchange with only one existent object
            assertFalse(client.exchangeObject("b1", "o1", "o2"));
            assertEquals(JH3Status.JH3_FAILURE, client.getStatus());

            // Create second object to be exchanged
            JH3Object dataObj2 = new JH3Object(Arrays.copyOfRange(data, 0, MEGABYTE), MEGABYTE);
            assertTrue(client.createObject("b1", "o2", dataObj2));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Exchange the two objects
            assertTrue(client.exchangeObject("b1", "o1", "o2"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Check info of first object
            objectInfo = client.infoObject("b1", "o1");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(MEGABYTE, objectInfo.getSize());

            readObj = client.readObject("b1", "o1");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(dataObj2.getData(), readObj.getData()));;

            // Check info of second object
            objectInfo = client.infoObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3 * MEGABYTE, objectInfo.getSize());

            readObj = client.readObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(readObj);
            assertTrue(Arrays.equals(dataObj.getData(), readObj.getData()));

            // Delete objects
            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(client.deleteObject("b1", "o2"));
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

    /**
     * Use create/write copy methods
     */
    @Test
    public void copyRangeTest(){
        try {
            ArrayList<String> buckets;
            ArrayList<String> objects;
            // Initialize client
            JH3 client = new JH3(storageURI, userId);

            // Create some random data and store them into an H3Object
            byte[] data = new byte[3 * MEGABYTE];
            new Random().nextBytes(data);
            JH3Object dataObj = new JH3Object(data, 3* MEGABYTE);

            // Check if there are any buckets
            buckets = client.listBuckets();

            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Create object copy from non-existent bucket
            assertEquals(-1, client.createObjectCopy("b1", "o1", 0, 0, "o2"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Create bucket
            assertTrue(client.createBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create object copy from non-existent object
            assertEquals(-1, client.createObjectCopy("b1", "o1", 0, 0, "o2"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Create the first object
            assertTrue(client.createObject("b1", "o1", dataObj));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create object from copy (zero size)
            assertEquals(0, client.createObjectCopy("b1", "o1", 0, 0, "o2"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create object from copy into existent object
            assertEquals(-1, client.createObjectCopy("b1", "o1", 0, 0, "o2"));
            assertEquals(JH3Status.JH3_FAILURE, client.getStatus());

            // Create object from part of other object
            //assertEquals(MEGABYTE, client.createObjectCopy("b1", "o1", 0, MEGABYTE, "o3"));
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());

            // List objects
            ArrayList<String> expected = new ArrayList<>();
            expected.add("o1");
            expected.add("o2");
            //expected.add("o3");
            objects = client.listObjects("b1", 0);
            Collections.sort(objects);
            assertEquals(expected, objects);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Get info of objects
            JH3ObjectInfo objectInfo = client.infoObject("b1", "o1");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(3* MEGABYTE, objectInfo.getSize());

            objectInfo = client.infoObject("b1", "o2");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(objectInfo);
            assertFalse(objectInfo.isCorrupt());
            assertEquals(0, objectInfo.getSize());

            // objectInfo = client.infoObject("b1", "o3");
            // assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            // assertNotNull(objectInfo);
            // assertFalse(objectInfo.isCorrupt());
            // assertEquals(MEGABYTE, objectInfo.getSize());


            // Write object from non-existent bucket
            assertEquals(-1, client.writeObjectCopy("b2", "o1", 0, 0, "o4", 0));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Write object from non-existent object
            assertEquals(-1, client.writeObjectCopy("b1", "o4", 0, 0, "o5", 0));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Try to overwrite
            assertEquals(-1, client.writeObjectCopy("b1", "o1", 0, 0, "o2", 0));
            assertEquals(JH3Status.JH3_FAILURE, client.getStatus());

            // Write to new object
            assertEquals(0, client.writeObjectCopy("b1", "o1", 0, 0, "o4", 0));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete objects
            assertTrue(client.deleteObject("b1", "o1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(client.deleteObject("b1", "o2"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            //assertTrue(client.deleteObject("b1", "o3"));
            //assertEquals(H3Status.H3_SUCCESS, client.getStatus());
            assertTrue(client.deleteObject("b1", "o4"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());


            // Delete bucket
            assertTrue(client.deleteBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());


        } catch (JH3Exception e) {
            e.printStackTrace();
        }
    }


}
