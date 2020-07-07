package gr.forth.ics.JH3lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Storage providers supported by H3lib.
 * @author  Giorgos Kalaentzis
 * @version 0.1-beta
 */
public enum JH3StoreType {

   /** Provider is set in the configuration file. */
    JH3_STORE_CONFIG(JH3Interface.StoreType.H3_STORE_CONFIG),
    /** Use mounted filesystem. */
    JH3_STORE_FILESYSTEM(JH3Interface.StoreType.H3_STORE_FILESYSTEM),
    /** Kreon cluster. */
    JH3_STORE_KREON(JH3Interface.StoreType.H3_STORE_KREON),
    /** RocksDB server. */
    JH3_STORE_ROCKSDB(JH3Interface.StoreType.H3_STORE_ROCKSDB),
    /** Redis cluster. */
    JH3_STORE_REDIS_CLUSTER(JH3Interface.StoreType.H3_STORE_REDIS_CLUSTER),
    /** Redis. */
    JH3_STORE_REDIS(JH3Interface.StoreType.H3_STORE_REDIS),
    /** IME cluster (not available). */
    JH3_STORE_IME(JH3Interface.StoreType.H3_STORE_IME),
    /** Not an option, used for iteration purposes. */
    JH3_STORE_NumOfStores(JH3Interface.StoreType.H3_STORE_NumOfStores);

    private final int storeType;
    private static final Map<Integer, JH3StoreType> lookup = new HashMap<>();

    // Map int -> JH3StoreType enum for easy lookup later
    static {
        for( JH3StoreType s : EnumSet.allOf(JH3StoreType.class)){
            lookup.put(s.getStoreType(), s);
        }
    }

    JH3StoreType(int storeType){
        this.storeType = storeType;
    }

    public int getStoreType() {
        return storeType;
    }

    /**
     *Get a JH3StoreType from its integer representation.
     * @param id  Integer representation of the store type
     * @return
     */
    public static JH3StoreType fromInt(int id) {
        return lookup.get(id);
    }
}