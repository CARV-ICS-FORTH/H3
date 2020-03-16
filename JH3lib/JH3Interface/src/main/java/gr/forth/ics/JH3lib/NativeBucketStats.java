package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * JNA representation of H3_BucketStats
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */

public class NativeBucketStats extends Structure {

    public NativeLong size;
    public long nObjects;
    public NativeTimespec lastAccess;
    public NativeTimespec lastModification;

    public NativeBucketStats() { super(); }

    /**
     * Create native bucket stats.
     * @param size                  The size of all objects contained in the bucket
     * @param nObjects              Number of objects contained in the bucket
     * @param lastAccess            Last time an object was accessed
     * @param lastModification      Last time an object was modified
     */
    public NativeBucketStats(NativeLong size, long nObjects,
                             NativeTimespec lastAccess, NativeTimespec lastModification) {
        super();
        this.size = size;
        this.nObjects = nObjects;
        this.lastAccess = lastAccess;
        this.lastModification = lastModification;
    }

    @Override
    protected List<String> getFieldOrder(){
        return Arrays.asList("size", "nObjects", "lastAccess", "lastModification");
    }

    /**
     * Create native bucket stats from the real native pointer.
     * @param peer Pointer value of the real native pointer
     */
    public NativeBucketStats(Pointer peer) { super(peer); }

    @Override
    public String toString() {
        return "NativeBucketStats{" +
                "size=" + size +
                ", nObjects=" + nObjects +
                ", lastAccess=" + lastAccess +
                ", lastModification=" + lastModification +
                '}';
    }

    public static abstract class ByReference extends NativeBucketStats implements Structure.ByReference{

    };

    public static abstract class ByValue extends NativeBucketStats implements Structure.ByValue{

    };

}
