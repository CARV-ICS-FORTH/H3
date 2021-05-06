package gr.forth.ics.JH3lib;

/**
 * JH3 Object information.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3ObjectInfo {
    private boolean corrupt;
    private long size;
    private JH3Timespec creation;
    private JH3Timespec lastAccess;
    private JH3Timespec lastModification;
    private JH3Timespec lastChange;
    private int mode;
    private int uid;
    private int gid;


    /**
     * Create Object information without attribute information
     * @param corrupt           Data are corrupt
     * @param size              Object size
     * @param creation          Creation timestamp
     * @param lastAccess        Last access timestamp
     * @param lastModification  Last modification timestamp
     */
   // public JH3ObjectInfo(boolean corrupt, long size, JH3Timespec creation, JH3Timespec lastAccess, JH3Timespec lastModification) {
   //     this.corrupt = corrupt;
   //     this.size = size;
   //     this.creation = creation;
   //     this.lastAccess = lastAccess;
   //     this.lastModification = lastModification;
   // }


    /**
     * Create Object information including attribute information
     * @param corrupt           Data are corrupt
     * @param size              Object size
     * @param creation          Creation timestamp
     * @param lastAccess        Last access timestamp
     * @param lastModification  Last modification timestamp
     * @param lastChange        Last time the object's attributes were changed
     * @param mode              File type and mode
     * @param uid               User ID
     * @param gid               Group ID
     */
    public JH3ObjectInfo(boolean corrupt, long size, JH3Timespec creation, JH3Timespec lastAccess, JH3Timespec lastModification,
     JH3Timespec lastChange, int mode, int uid, int gid) {
        this.corrupt = corrupt;
        this.size = size;
        this.creation = creation;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
        this.lastChange = lastChange;
        this.mode = mode;
        this.uid = uid;
        this.gid = gid;
    }
    

    /**
     * Check if the stored object is corrupt.
     * @return the corruption flag.
     */
    public boolean isCorrupt() {
        return corrupt;
    }

    /**
     * Get the size of the JH3Object.
     * @return the size of the object.
     */
    public long getSize() {
        return size;
    }

    /**
     * Get the creation timestamp.
     * @return the creation timestamp.
     */
    public JH3Timespec getCreation() {
        return creation;
    }

    /**
     * Get the timestamp of when the JH3Object was last accessed.
     * @return the last accessed timestamp.
     */
    public JH3Timespec getLastAccess() {
        return lastAccess;
    }

    /**
     * Get the timestamp of when the JH3Object was last modified.
     * @return the last modified timestamp.
     */
    public JH3Timespec getLastModification() {
        return lastModification;
    }

    /**
     * Get the last time the JH3Object's attributes were changed.
     */
    public JH3Timespec getLastChange(){
        return lastChange;
    }

    /**
     * Get the attribute mode of JH3Object.
     */
    public int getMode(){
        return mode;
    }

    /**
     * Get the user ID of the JH3Object.
     */
    public int getUid(){
        return uid;
    }

    public int getGid(){
        return gid;
    }


    @Override
    public String toString() {
        return "JH3ObjectInfo{" +
            " corrupt='" + corrupt + "'" +
            ", size='" + size + "'" +
            ", creation='" + creation + "'" +
            ", lastAccess='" + lastAccess + "'" +
            ", lastModification='" + lastModification + "'" +
            ", lastChange='" + lastChange + "'" +
            ", mode='" + mode + "'" +
            ", uid='" + uid + "'" +
            ", gid='" + gid + "'" +
            "}";
    }
}
