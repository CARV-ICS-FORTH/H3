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
    public NativeLong creation;
    public NativeLong lastAccess;
    public NativeLong lastModification;

    public NativeObjectInfo() { super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("isBad", "size", "creation", "lastAccess", "lastModification");
    }

    /**
     * Create native object info.
     * @param isBad                 Data are corrupt
     * @param size                  Object size
     * @param creation              Creation timestamp
     * @param lastAccess            Last access timestamp
     * @param lastModification      Last modification timestamp
     */
    public NativeObjectInfo(byte isBad, NativeLong size, NativeLong creation,
                            NativeLong lastAccess, NativeLong lastModification) {
        super();
        this.isBad = isBad;
        this.size = size;
        this.creation = creation;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
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