package gr.forth.ics.JH3lib;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.*;

public class JH3BucketTest {

    // Initialize storage URI; if H3LIB_STORAGE_URI is not set, use default uri (local filesystem: /tmp/h3) 
    private static String storageURI = null;
    static {
      storageURI = System.getenv("H3LIB_STORAGE_URI");
      if(storageURI == null){
        storageURI = "file:///tmp/h3";
      }
    }

    private int userId = 0;                                                 // Dummy userId


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
      } catch(JH3Exception e){}
    }

    /**
     * List / create / delete a bucket
     */
    @Test
    public void simpleTest(){
        try {
            ArrayList<String> buckets;

            // Initialize client
            JH3 client = new JH3(storageURI, userId);

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Create a bucket
            assertTrue(client.createBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Create the same bucket
            assertFalse(client.createBucket("b1"));
            assertEquals(JH3Status.JH3_EXISTS, client.getStatus());

            // Get info of bucket without stats
            JH3BucketInfo bucketInfo = client.infoBucket("b1");
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNull(bucketInfo.getStats());
            assertNotEquals(0, bucketInfo.getCreation());

            // Get info of bucket with stats
            bucketInfo = client.infoBucket("b1", true);
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertNotNull(bucketInfo.getStats());
            assertEquals(0, bucketInfo.getStats().getSize());
            assertEquals(0, bucketInfo.getStats().getNumObjects());

            ArrayList<String> expected = new ArrayList<>();
            expected.add("b1");

            // List buckets should contain the bucket now
            assertEquals(expected, client.listBuckets());
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete a bucket
            assertTrue(client.deleteBucket("b1"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete the same bucket
            assertFalse(client.deleteBucket("b1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Get info of non-existent bucket
            assertNull(client.infoBucket("b1"));
            assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

        } catch (JH3Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Pass invalid arguments
     */
    @Test
    public void argumentsTest(){
        try {
            // Initialize client
            JH3 client = new JH3(storageURI, userId);

            // Bucket with empty name
            assertFalse(client.createBucket(""));
            assertEquals(JH3Status.JH3_INVALID_ARGS, client.getStatus());

            // Bucket with null
            try {
                client.createBucket(null);
            } catch (NullPointerException e) {
                // Null pointer is the expected exception; don't handle other exceptions
            }

            // Large bucket name
            String largeName = new String(new char[JH3.H3_BUCKET_NAME_SIZE + 1]).replace("\0", "a");
            assertFalse(client.createBucket(largeName));
            assertEquals(JH3Status.JH3_NAME_TOO_LONG, client.getStatus());

            // Invalid bucket names
            assertFalse(client.createBucket("/bucketId"));
            assertEquals(JH3Status.JH3_INVALID_ARGS, client.getStatus());

            assertTrue(client.createBucket("\bucketId"));
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            assertTrue(client.deleteBucket("\bucketId")); 
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

        } catch (JH3Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Manage many buckets
     */
    @Test
    public void manyBucketsTest(){

        try {
            int count = 100;
            ArrayList<String> buckets;

            // Initialize client
            JH3 client = new JH3(storageURI, userId);

            // Check if there are any buckets
            buckets = client.listBuckets();
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

            // Create buckets
            for(int i = 0; i < count; i++){
                assertTrue(client.createBucket("bucket" + i));
                assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            }

            // Randomly create 10 already existing buckets
            Random rn = new Random();
            for(int i = 0; i < 10; i++){
                assertFalse(client.createBucket("bucket" + rn.nextInt(count)));
                assertEquals(JH3Status.JH3_EXISTS, client.getStatus());
            }

            // Get info of all buckets (without stats)
            for(int i = 0; i < count; i++){
                JH3BucketInfo bucketInfo = client.infoBucket("bucket" + i);
                assertNull(bucketInfo.getStats());
                assertNotEquals(0, bucketInfo.getCreation());
            }

            ArrayList<String> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                expected.add("bucket" + i);
            }

            // List all buckets
            assertEquals(expected, client.listBuckets());
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());

            // Delete all buckets
            for (int i = 0; i < count; i++) {
                assertTrue(client.deleteBucket("bucket" + i));
                assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            }

            // Randomly delete already deleted buckets
            for (int i = 0; i < 10; i++) {
                assertFalse(client.deleteBucket("bucket" + i));
                assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());
            }

            // Randomly get info of non-existent buckets
            for (int i = 0; i < 10; i++) {
                assertNull(client.infoBucket("bucket" + rn.nextInt(count)));
                assertEquals(JH3Status.JH3_NOT_EXISTS, client.getStatus());
            }

            // List buckets should be empty
            buckets = client.listBuckets();
            assertEquals(JH3Status.JH3_SUCCESS, client.getStatus());
            assertTrue(buckets.isEmpty());

        } catch (JH3Exception e) {
            e.printStackTrace();
        }

    }
}
