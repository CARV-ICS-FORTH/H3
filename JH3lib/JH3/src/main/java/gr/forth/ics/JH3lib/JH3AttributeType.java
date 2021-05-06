package gr.forth.ics.JH3lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * The attribute type of a bucket / object.
 *
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public enum JH3AttributeType {

    /** Owner attributes. */
    JH3_ATTRIBUTE_OWNER(JH3Interface.AttributeType.H3_ATTRIBUTE_OWNER),
    /** Permission attribute. */
    JH3_ATTRIBUTE_PERMISSIONS(JH3Interface.AttributeType.H3_ATTRIBUTE_PERMISSIONS),
    /** Not an option, used for iteration purposes. */
    JH3_NumOfAttributes(JH3Interface.AttributeType.H3_NumOfAttributes);

    private final int type;
    private static final Map<Integer, JH3AttributeType> lookup = new HashMap<>();

    // Map int -> JH3AttributeType enum for easy lookup
    static {
        for(JH3AttributeType t : EnumSet.allOf(JH3AttributeType.class)){
            lookup.put(t.getAttributeType(), t);
        }
    }
    JH3AttributeType(int type){
        this.type = type;
    }

    public int getAttributeType(){
        return type;
    }

    /**
     * Get a JH3AttributeType from its integer representation
     * @param id Integer representation of the attribute type
     * @return  The attribute type as enum
     */
    public static JH3AttributeType fromInt(int id){
        return lookup.get(id);
    }
}
