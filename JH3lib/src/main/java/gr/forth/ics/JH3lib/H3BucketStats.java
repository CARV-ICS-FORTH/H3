package gr.forth.ics.JH3lib;

/**
 * Brief bucket statistics
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3BucketStats {
    private long size;                 // The size of all objects contained in the bucket
    private long nObjects;             // Number of objects contained in the bucket
    private long lastAccess;           // Last time an object was accessed
    private long lastModification;     // Last time an object was modified

    public H3BucketStats(long size, long nObjects, long lastAccess, long lastModification) {
        this.size = size;
        this.nObjects = nObjects;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
    }

    public long getSize() {
        return size;
    }

    public long getNumObjects() {
        return nObjects;
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public long getLastModification() {
        return lastModification;
    }

    @Override
    public String toString() {
        return "H3BucketStats{" +
                "size=" + size +
                ", nObjects=" + nObjects +
                ", lastAccess=" + lastAccess +
                ", lastModification=" + lastModification +
                '}';
    }
}
