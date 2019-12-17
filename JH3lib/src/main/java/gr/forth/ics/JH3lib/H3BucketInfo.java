package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;


public class H3BucketInfo extends Structure {

    public Pointer name;
    public NativeLong size;
    public long nObjects;
    public NativeLong creation;

    public H3BucketInfo(){ super(); }

    protected List<String> getFieldOrder() {
        return Arrays.asList("name", "size", "nObjects", "creation");
    }

    public H3BucketInfo(Pointer name, NativeLong size, long nObjects){
        super();
        this.name = name;
        this.size = size;
        this.nObjects = nObjects;
    }

    public H3BucketInfo(Pointer peer) { super(peer); }

    public static class ByReference extends H3BucketInfo implements Structure.ByReference {

    };

    public static class ByValue extends H3BucketInfo implements Structure.ByValue {

    };

}
