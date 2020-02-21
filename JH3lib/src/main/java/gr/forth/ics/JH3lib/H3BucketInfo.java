package gr.forth.ics.JH3lib;


/**
 * Bucket statistics
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3BucketInfo {

    private long creation;              // Creation timestamp
    private H3BucketStats stats;        // Aggregate object statistics

    public H3BucketInfo(long creation, H3BucketStats stats) {
        this.creation = creation;
        this.stats = stats;
    }

    public H3BucketInfo(long creation) {
        this.creation = creation;
        this.stats = null;
    }

    public long getCreation() {
        return creation;
    }

    public H3BucketStats getStats() {
        return stats;
    }

    @Override
    public String toString() {
        return "H3BucketInfo{" +
                "creation=" + creation +
                ", stats=" + stats +
                '}';
    }
}
