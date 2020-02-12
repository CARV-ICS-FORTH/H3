package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * Brief bucket statistics
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */

public class H3BucketStats extends Structure {

    public NativeLong size;                 //!< The size of all objects contained in the bucket
    public long nObjects;                   //!< Number of objects contained in the bucket
    public NativeLong lastAccess;           //!< Last time an object was accessed
    public NativeLong lastModification;     //!< Last time an object was modified

    public H3BucketStats() { super(); }

    /**
     * @param size                  The size of all objects contained in the bucket <br>
     * @param nObjects              Number of objects contained in the bucket       <br>
     * @param lastAccess            Last time an object was accessed                <br>
     * @param lastModification      Last time an object was modified
     */
    public H3BucketStats(NativeLong size, long nObjects,
                         NativeLong lastAccess, NativeLong lastModification) {
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
    public H3BucketStats(Pointer peer) { super(peer); }

    public static abstract class ByReference extends H3BucketStats implements Structure.ByReference{

    };

    public static abstract class ByValue extends H3BucketStats implements Structure.ByValue{

    };

}
