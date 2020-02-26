package gr.forth.ics.JH3lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * The status of a h3lib operation.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public enum H3Status {

    /** Operation failed. */
    H3_FAILURE (JH3libInterface.Status.H3_FAILURE),
    /** Arguments are missing or malformed. */
    H3_INVALID_ARGS (JH3libInterface.Status.H3_INVALID_ARGS),
    /** External (store provider) error. */
    H3_STORE_ERROR (JH3libInterface.Status.H3_STORE_ERROR),
    /** Bucket or object already exists. */
    H3_EXISTS (JH3libInterface.Status.H3_EXISTS),
    /** Bucket or object does not exist. */
    H3_NOT_EXISTS (JH3libInterface.Status.H3_NOT_EXISTS),
    /** Operation succeeded. */
    H3_SUCCESS (JH3libInterface.Status.H3_SUCCESS),
    /** Operation succeeded though there are more data to retrieve. */
    H3_CONTINUE (JH3libInterface.Status.H3_CONTINUE);

    private final int status;
    private static final Map<Integer,H3Status> lookup = new HashMap<>();


    // Map int -> H3Status enum for easy lookup later
    static {
        for( H3Status s : EnumSet.allOf(H3Status.class)){
            lookup.put(s.getStatus(), s);
        }
    }

    H3Status(int status){
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    /**
     * Get a H3Status enum from its integer representation.
     * @param id    Integer representation of the status
     * @return      The status as enum
     */
    public static H3Status fromInt(int id) {
       return lookup.get(id);
    }
}
