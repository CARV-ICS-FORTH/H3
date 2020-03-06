package gr.forth.ics.JH3lib;


/**
 * Bucket statistics
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3BucketInfo {

    private long creation;
    private H3BucketStats stats;

    /**
     * Create a BucketInfo object with statistics.
     * @param creation  Creation timestamp
     * @param stats     Aggregate object statistics
     */
    public H3BucketInfo(long creation, H3BucketStats stats) {
        this.creation = creation;
        this.stats = stats;
    }

    /**
     * Create a BucketInfo object without statistics. Stats are set to null.
     * @param creation  Creation timestamp
     */
    public H3BucketInfo(long creation) {
        this.creation = creation;
        this.stats = null;
    }

    /**
     * Get the creation timestamp.
     * @return The creation timestamp.
     */
    public long getCreation() {
        return creation;
    }

    /**
     * Get the aggregate bucket stats.
     * @return The aggregate bucket stats if they were collected, <code>null</code> otherwise;
     */
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
