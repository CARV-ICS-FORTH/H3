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
public class NativeBucketInfo extends Structure {

    public NativeLong creation;    //!< Creation timestamp
    public NativeBucketStats stats;     //!< Aggregate object statistics

    public NativeBucketInfo(){ super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("creation", "stats");
    }

    @Override
    public String toString() {
        return "NativeBucketInfo{" +
                "creation=" + creation +
                ", stats=" + stats +
                '}';
    }

    /**
     * @param creation  Creation timestamp
     * @param stats     Aggregate object statistics
     */
    public NativeBucketInfo(NativeLong creation, NativeBucketStats stats) {
        super();
        this.creation = creation;
        this.stats = stats;
    }

    public NativeBucketInfo(Pointer peer) { super(peer); }

    public static class ByReference extends NativeBucketInfo implements Structure.ByReference {

    };

    public static class ByValue extends NativeBucketInfo implements Structure.ByValue {

    };

}
