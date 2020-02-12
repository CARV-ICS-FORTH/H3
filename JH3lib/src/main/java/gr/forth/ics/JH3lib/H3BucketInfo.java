package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

/**
 * Brief bucket information
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3BucketInfo extends Structure {

    public NativeLong creation;    //!< Creation timestamp
    public H3BucketStats stats;     //!< Aggregate object statistics

    public H3BucketInfo(){ super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("creation", "stats");
    }

    /**
     * @param creation  Creation timestamp
     * @param stats     Aggregate object statistics
     */
    public H3BucketInfo(NativeLong creation, H3BucketStats stats) {
        super();
        this.creation = creation;
        this.stats = stats;
    }

    public H3BucketInfo(Pointer peer) { super(peer); }

    public static class ByReference extends H3BucketInfo implements Structure.ByReference {

    };

    public static class ByValue extends H3BucketInfo implements Structure.ByValue {

    };

}
