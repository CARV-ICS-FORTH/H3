package gr.forth.ics.JH3lib;

public class H3ObjectInfo {
    private boolean corrupt;             // Data are corrupt
    private long size;                 // Object size
    private long creation;             // Creation timestamp
    private long lastAccess;           // Last access timestamp
    private long lastModification;     // Last modification timestamp

    public H3ObjectInfo(boolean corrupt, long size, long creation, long lastAccess, long lastModification) {
        this.corrupt = corrupt;
        this.size = size;
        this.creation = creation;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
    }

    public boolean isCorrupt() {
        return corrupt;
    }

    public long getSize() {
        return size;
    }

    public long getCreation() {
        return creation;
    }

    public long getLastAccess() {
        return lastAccess;
    }

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
