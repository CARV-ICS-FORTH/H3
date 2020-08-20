package gr.forth.ics.JH3lib;

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

    public NativeTimespec creation;
    public NativeBucketStats stats;

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
     * Create native bucket info
     * @param creation  Creation timestamp
     * @param mode      Bucket type and mode (used by h3fuse)
     * @param stats     Aggregate object statistics
     */
    public NativeBucketInfo(NativeTimespec creation, NativeBucketStats stats) {
        super();
        this.creation = creation;
        this.stats = stats;
    }

    /**
     * Create native bucket info from the real native pointer
     * @param peer Pointer value of the real native pointer
     */
    public NativeBucketInfo(Pointer peer) { super(peer); }

    public static class ByReference extends NativeBucketInfo implements Structure.ByReference {

    };

    public static class ByValue extends NativeBucketInfo implements Structure.ByValue {

    };

}
