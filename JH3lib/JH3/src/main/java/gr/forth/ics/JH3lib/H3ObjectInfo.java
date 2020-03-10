package gr.forth.ics.JH3lib;

/**
 * H3Object information.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3ObjectInfo {
    private boolean corrupt;
    private long size;
    private long creation;
    private long lastAccess;
    private long lastModification;

    /**
     * @param corrupt           Data are corrupt
     * @param size              Object size
     * @param creation          Creation timestamp
     * @param lastAccess        Last access timestamp
     * @param lastModification  Last modification timestamp
     */
    public H3ObjectInfo(boolean corrupt, long size, long creation, long lastAccess, long lastModification) {
        this.corrupt = corrupt;
        this.size = size;
        this.creation = creation;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
    }

    /**
     * Check if the stored object is corrupt.
     * @return the corruption flag.
     */
    public boolean isCorrupt() {
        return corrupt;
    }

    /**
     * Get the size of the H3Object.
     * @return the size of the object.
     */
    public long getSize() {
        return size;
    }

    /**
     * Get the creation timestamp.
     * @return the creation timestamp.
     */
    public long getCreation() {
        return creation;
    }

    /**
     * Get the timestamp of when the H3Object was last accessed.
     * @return the last accessed timestamp.
     */
    public long getLastAccess() {
        return lastAccess;
    }

    /**
     * Get the timestamp of when the H3Object was last modified.
     * @return the last modified timestamp.
     */
    public long getLastModification() {
        return lastModification;
    }

    @Override
    public String toString() {
        return "H3PartInfo{" +
                "corrupt=" + corrupt +
                ", size=" + size +
                ", creation=" + creation +
                ", lastAccess=" + lastAccess +
                ", lastModification=" + lastModification +
                '}';
    }
}
