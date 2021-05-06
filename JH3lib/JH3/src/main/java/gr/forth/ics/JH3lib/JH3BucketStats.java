package gr.forth.ics.JH3lib;

/**
 * Brief bucket statistics.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3BucketStats {
    private long size;
    private long nObjects;
    private JH3Timespec lastAccess;
    private JH3Timespec lastModification;

    /**
     * Create a BucketStats object.
     * @param size              The size of all objects contained in the bucket
     * @param nObjects          Number of objects contained in the bucket
     * @param lastAccess        Last time an object was accessed
     * @param lastModification  Last time an object was modified
     */
    public JH3BucketStats(long size, long nObjects, JH3Timespec lastAccess, JH3Timespec lastModification) {
        this.size = size;
        this.nObjects = nObjects;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
    }

    /**
     * Get the bucket size.
     * @return The bucket size.
     */
    public long getSize() {
        return size;
    }

    /**
     * Get the number of objects that are hosted in the bucket.
     * @return The number of objects.
     */
    public long getNumObjects() {
        return nObjects;
    }

    /**
     * Get the timestamp when an object was last accessed.
     * @return the last access timestamp.
     */
    public JH3Timespec getLastAccess() {
        return lastAccess;
    }

    /**
     * Get the timestamp when an object was last modified.
     * @return the last modified timestamp.
     */
    public JH3Timespec getLastModification() {
        return lastModification;
    }

    @Override
    public String toString() {
        return "JH3BucketStats{" +
                "size=" + size +
                ", nObjects=" + nObjects +
                ", lastAccess=" + lastAccess +
                ", lastModification=" + lastModification +
                '}';
    }
}
