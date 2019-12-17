package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

public class H3MultipartInfo extends Structure {
    public int partId;
    public NativeLong size;
    public H3MultipartInfo() {
        super();
    }
    protected List<String> getFieldOrder() {
        return Arrays.asList("partId", "size");
    }

    public H3MultipartInfo(int partId, NativeLong size) {
        super();
        this.partId = partId;
        this.size = size;
    }
    public H3MultipartInfo(Pointer peer) {
        super(peer);
    }
    public static class ByReference extends H3MultipartInfo implements Structure.ByReference {

    };
    public static class ByValue extends H3MultipartInfo implements Structure.ByValue {

    };
}
