package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

public class H3ObjectInfo extends Structure {
    public Pointer name;
    public byte isBad;
    public NativeLong size;
    public NativeLong lastAccess;
    public NativeLong lastModification;

   public H3ObjectInfo() { super(); }

    protected List<String> getFieldOrder() {
        return Arrays.asList("name", "isBad", "size", "lastAccess", "lastModification");
    }

    public H3ObjectInfo(Pointer name, byte isBad, NativeLong size) {
        super();
        this.name = name;
        this.isBad = isBad;
        this.size = size;
    }

    public H3ObjectInfo(Pointer peer) {
        super(peer);
    }
    public static class ByReference extends H3ObjectInfo implements Structure.ByReference {

    };
    public static class ByValue extends H3ObjectInfo implements Structure.ByValue {

    };
}