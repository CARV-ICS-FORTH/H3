package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * JNA representation of H3_ObjectInfo
 * @author  Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class NativeObjectInfo extends Structure {
    public byte isBad;                      //!< Data are corrupt
    public NativeLong size;                 //!< Object size
    public NativeLong creation;             //!< Creation timestamp
    public NativeLong lastAccess;           //!< Last access timestamp
    public NativeLong lastModification;     //!< Last modification timestamp

    public NativeObjectInfo() { super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("isBad", "size", "creation", "lastAccess", "lastModification");
    }

    /**
     *
     * @param isBad                 Data are corrupt                <br>
     * @param size                  Object size                     <br>
     * @param creation              Creation timestamp              <br>
     * @param lastAccess            Last access timestamp           <br>
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

    public NativeObjectInfo(Pointer peer) {
        super(peer);
    }
    public static class ByReference extends NativeObjectInfo implements Structure.ByReference {

    };
    public static class ByValue extends NativeObjectInfo implements Structure.ByValue {

    };
}