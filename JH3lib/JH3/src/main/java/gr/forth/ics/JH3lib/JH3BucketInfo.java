package gr.forth.ics.JH3lib;


/**
 * Bucket statistics
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3BucketInfo {

    private JH3Timespec creation;
    private JH3BucketStats stats;

    /**
     * Create a BucketInfo object with statistics.
     * @param creation  Creation timestamp
     * @param stats     Aggregate object statistics
     */
    public JH3BucketInfo(JH3Timespec creation, JH3BucketStats stats) {
        this.creation = creation;
        this.stats = stats;
    }

    /**
     * Create a BucketInfo object without statistics. Stats are set to null.
     * @param creation  Creation timestamp
     */
    public JH3BucketInfo(JH3Timespec creation) {
        this.creation = creation;
        this.stats = null;
    }

    /**
     * Get the creation timestamp.
     * @return The creation timestamp.
     */
    public JH3Timespec getCreation() {
        return creation;
    }

    /**
     * Get the aggregate bucket stats.
     * @return The aggregate bucket stats if they were collected, <code>null</code> otherwise;
     */
    public JH3BucketStats getStats() {
        return stats;
    }

    @Override
    public String toString() {
        return "JH3BucketInfo{" +
                "creation=" + creation +
                ", stats=" + stats +
                '}';
    }
}
