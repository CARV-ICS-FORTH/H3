package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * JNA representation of H3_ObjectInfo.
 * @author  Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class NativeObjectInfo extends Structure {
    public byte isBad;
    public NativeLong size;
    public NativeTimespec creation;
    public NativeTimespec lastAccess;
    public NativeTimespec lastModification;
    public NativeTimespec lastChange;
    public int mode;        /* mode_t in native h3lib */
    public int uid;         /* uid_t in native h3lib */
    public int gid;         /* gid_t in native h3lib */                   


    public NativeObjectInfo() { super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("isBad", "size", "creation", "lastAccess",
         "lastModification", "lastChange", "mode", "uid", "gid");
    }

    /**
     * Create native object info.
     * @param isBad                 Data are corrupt
     * @param size                  Object size
     * @param creation              Creation timestamp
     * @param lastAccess            Last access timestamp
     * @param lastModification      Last modification timestamp
     * @param lastChange            The last time the object's attributes were changed (e.g. permissions)
     * @param mode                  File type and mode (used by h3fuse)
     * @param uid                   User ID (used by h3fuse)
     * @param gid                   Group ID (used by h3fuse)
     */
    public NativeObjectInfo(byte isBad, NativeLong size, NativeTimespec creation, NativeTimespec lastAccess,
        NativeTimespec lastModification, NativeTimespec lastChange, int mode, int uid, int gid) {
        super();
        this.isBad = isBad;
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
     * Create native object info from the real native pointer.
     * @param peer Pointer value of the real native pointer
     */
    public NativeObjectInfo(Pointer peer) {
        super(peer);
    }

    public static class ByReference extends NativeObjectInfo implements Structure.ByReference {

    };
    public static class ByValue extends NativeObjectInfo implements Structure.ByValue {

    };
}