package gr.forth.ics.JH3lib;

import java.util.Arrays;
import java.util.List;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.Union;

/**
 * JNA Representation of H3_Attribute
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class NativeAttribute extends Structure {

    public int type;
    public AttributeUnion unionval;

    @Override
    protected List<String> getFieldOrder(){
        return Arrays.asList("type", "unionval");
    }

    public static class AttributeUnion extends Union {

        /** Permissions in octal mode similar to chmod() */
        public int mode;
        /** Structure that holds user, group ID */
        public AttributeStruct structval;

        public static class AttributeStruct extends Structure {
            /** User ID, adhering to chown() semanics */
            public int uid;
            /** Group ID, adhering to chown() semantics */
            public int gid;

            @Override
            protected List<String> getFieldOrder(){
                return Arrays.asList("uid", "gid");
            }
            public AttributeStruct(){
                super();
            }

            public AttributeStruct(int uid, int gid){
                super();
                this.uid = uid;
                this.gid = gid;
            }

            public AttributeStruct(Pointer peer) {
                super(peer);
            }

            public static class ByReference extends AttributeStruct implements Structure.ByReference{};

            public static class ByValue extends AttributeStruct implements Structure.ByValue {};

        };
 
        public AttributeUnion(){
            super();
        }

        public AttributeUnion(int mode){
            super();
            this.mode = mode;
            setType(int.class);
        }

        public AttributeUnion(AttributeStruct structval){
            super();
            this.structval = structval;
            setType(AttributeStruct.class);
        }

        public AttributeUnion(Pointer peer){
            super(peer);
        }

        public static class ByReference extends AttributeUnion implements Structure.ByReference{};

        public static class ByValue extends AttributeUnion implements Structure.ByValue{};
    };

    public NativeAttribute(){
        super();
    }

    public NativeAttribute(int type, AttributeUnion unionval){
        super();
        this.type = type;
        this.unionval = unionval;
    }

    public NativeAttribute(Pointer peer){
        super(peer);
    }

    public static class ByReference extends NativeAttribute implements Structure.ByReference{};

    public static class ByValue extends NativeAttribute implements Structure.ByValue{};
}