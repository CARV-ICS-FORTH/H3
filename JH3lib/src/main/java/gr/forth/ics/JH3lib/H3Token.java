package gr.forth.ics.JH3lib;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

// H3Token struct
public class H3Token extends  Structure{
    public int userId;

    public H3Token() {
        super();
    }

    public H3Token(int userId){
        super();
        this.userId = userId;
    }

    public H3Token(Pointer peer){
        super(peer);
    }
    // Order of fields in struct
    protected List<String> getFieldOrder(){
        return Arrays.asList("userId");
    }

    public static class ByReference extends H3Token implements Structure.ByReference {

    };

    public static class ByValue extends  H3Token implements Structure.ByValue {

    };
}