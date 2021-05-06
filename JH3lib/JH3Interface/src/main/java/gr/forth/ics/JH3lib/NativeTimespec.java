package gr.forth.ics.JH3lib;

import com.sun.jna.NativeLong;
import com.sun.jna.Structure;
import com.sun.jna.Pointer;

import java.util.Arrays;
import java.util.List;

/**
 * JNA representation of timespec struct. 
 * This struct holds an interval broken down into seconds and nanoseconds.
 */
public class NativeTimespec extends Structure {

    public NativeLong tv_sec;
    public NativeLong tv_nsec;

    public NativeTimespec(){ super();}

    /**
     * Create a Timespec object.
     * @param tv_sec    The seconds of the interval.
     * @param tv_nsec   The nanoseconds of the interval.
     */
    public NativeTimespec(NativeLong tv_sec, NativeLong tv_nsec){
        super();
        this.tv_sec = tv_sec;
        this.tv_nsec = tv_nsec;
    }

    /**
     * Create native object info from the real native pointer.
     * @param peer Pointer value of the real native pointer
     */
    public NativeTimespec(Pointer peer){
        super(peer);
    }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("tv_sec", "tv_nsec");
    }

    public static class ByReference extends NativeTimespec implements Structure.ByReference {

    };
    public static class ByValue extends NativeTimespec implements Structure.ByValue {

    };
}
