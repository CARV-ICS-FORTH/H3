package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * Brief information on individual parts of a multi-part object
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */

public class H3PartInfo extends Structure {
    public int partNumber;          //!< Part number
    public NativeLong size;         //!< Part size

    public H3PartInfo() {
        super();
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("partNumber", "size");
    }

    /**
     *
     * @param partNumber    Part number <br>
     * @param size          Part size
     */
    public H3PartInfo(int partNumber, NativeLong size) {
        super();
        this.partNumber = partNumber;
        this.size = size;
    }
    public H3PartInfo(Pointer peer) {
        super(peer);
    }
    public static class ByReference extends H3PartInfo implements Structure.ByReference {

    };
    public static class ByValue extends H3PartInfo implements Structure.ByValue {

    };
}
