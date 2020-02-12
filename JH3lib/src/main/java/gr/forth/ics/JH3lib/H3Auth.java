package gr.forth.ics.JH3lib;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

public class H3Auth extends Structure {
    public int userId;

    public H3Auth() { super(); }

    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("userId");
    }

    public H3Auth(int userId){
        super();
        this.userId = userId;
    }

    public H3Auth(Pointer peer) { super(peer); }

    public static class ByReference extends H3Auth implements Structure.ByReference{

    };

    public static class ByValue extends H3Auth implements Structure.ByValue{

    };

}
