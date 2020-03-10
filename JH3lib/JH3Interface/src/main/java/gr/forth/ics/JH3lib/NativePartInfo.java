package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * JNA representation of H3_PartInfo
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class NativePartInfo extends Structure {
    public int partNumber;          //!< Part number
    public NativeLong size;         //!< Part size

    public NativePartInfo() {
        super();
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("partNumber", "size");
    }

    /**
     * Create native part info.
     * @param partNumber    Part number
     * @param size          Part size
     */
    public NativePartInfo(int partNumber, NativeLong size) {
        super();
        this.partNumber = partNumber;
        this.size = size;
    }

    /**
     * Create native part info from the real native pointer.
     * @param peer Pointer value of the real native pointer
     */
    public NativePartInfo(Pointer peer) {
        super(peer);
    }

    public static class ByReference extends NativePartInfo implements Structure.ByReference {

    };
    public static class ByValue extends NativePartInfo implements Structure.ByValue {

    };

    @Override
    public String toString() {
        return "NativePartInfo{" +
                "partNumber=" + partNumber +
                ", size=" + size +
                '}';
    }
}
