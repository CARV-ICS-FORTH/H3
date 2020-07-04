package gr.forth.ics.JH3lib;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

/**
 * JNA representation of H3_Auth
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class NativeAuth extends Structure {
    public int userId;

    public NativeAuth() { super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("userId");
    }

    /**
     * Create a native authentication token.
     * @param userId The id of the user
     */
    public NativeAuth(int userId){
        super();
        this.userId = userId;
    }

    /**
     * Create a native authentication token from the real native pointer.
     * @param peer Pointer value of the real native pointer
     */
    public NativeAuth(Pointer peer) { super(peer); }

    public static class ByReference extends NativeAuth implements Structure.ByReference{

    };

    public static class ByValue extends NativeAuth implements Structure.ByValue{

    };

}
