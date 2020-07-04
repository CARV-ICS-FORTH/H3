package gr.forth.ics.JH3lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * The status of a h3lib operation.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public enum JH3Status {

    /** Operation failed. */
    JH3_FAILURE(JH3Interface.Status.H3_FAILURE),
    /** Arguments are missing or malformed. */
    JH3_INVALID_ARGS(JH3Interface.Status.H3_INVALID_ARGS),
    /** External (store provider) error. */
    JH3_STORE_ERROR(JH3Interface.Status.H3_STORE_ERROR),
    /** Bucket or object already exists. */
    JH3_EXISTS(JH3Interface.Status.H3_EXISTS),
    /** Bucket or object does not exist. */
    JH3_NOT_EXISTS(JH3Interface.Status.H3_NOT_EXISTS),
    /** Bucker or object name is too long */
    JH3_NAME_TOO_LONG(JH3Interface.Status.H3_NAME_TOO_LONG),
    /** Bucket is not empty. */
    JH3_NOT_EMPTY(JH3Interface.Status.H3_NOT_EMPTY),
    /** Operation succeeded. */
    JH3_SUCCESS(JH3Interface.Status.H3_SUCCESS),
    /** Operation succeeded though there are more data to retrieve. */
    JH3_CONTINUE(JH3Interface.Status.H3_CONTINUE);

    private final int status;
    private static final Map<Integer, JH3Status> lookup = new HashMap<>();


    // Map int -> JH3Status enum for easy lookup later
    static {
        for( JH3Status s : EnumSet.allOf(JH3Status.class)){
            lookup.put(s.getStatus(), s);
        }
    }

    JH3Status(int status){
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    /**
     * Get a JH3Status enum from its integer representation.
     * @param id    Integer representation of the status
     * @return      The status as enum
     */
    public static JH3Status fromInt(int id) {
       return lookup.get(id);
    }
}
