package gr.forth.ics.JH3lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum H3Status {

    H3_FAILURE (JH3libInterface.H3_FAILURE),            //!< Operation failed
    H3_INVALID_ARGS (JH3libInterface.H3_INVALID_ARGS),  //!< Arguments are missing or malformed
    H3_STORE_ERROR (JH3libInterface.H3_STORE_ERROR),    //!< External (store provider) error
    H3_EXISTS (JH3libInterface.H3_EXISTS),              //!< Bucket or object already exists
    H3_NOT_EXISTS (JH3libInterface.H3_NOT_EXISTS),      //!< Bucket or object does not exist
    H3_SUCCESS (JH3libInterface.H3_SUCCESS),            //!< Operation succeeded
    H3_CONTINUE (JH3libInterface.H3_CONTINUE);          //!< Operation succeeded though there are more data to retrieve

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

    public static H3Status fromInt(int id) {
       return lookup.get(id);
    }
}
