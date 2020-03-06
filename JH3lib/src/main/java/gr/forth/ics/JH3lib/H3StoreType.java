package gr.forth.ics.JH3lib;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Storage providers supported by H3.
 * @author  Giorgos Kalaentzis
 * @version 0.1-beta
 */
public enum H3StoreType {

   /** Provider is set in the configuration file. */
    H3_STORE_CONFIG (JH3libInterface.StoreType.H3_STORE_CONFIG),
    /** Use mounted filesystem. */
    H3_STORE_FILESYSTEM (JH3libInterface.StoreType.H3_STORE_FILESYSTEM),
    /** Kreon cluster (not available). */
    H3_STORE_KREON (JH3libInterface.StoreType.H3_STORE_KREON),
    /** RocksDB server (not available). */
    H3_STORE_ROCKSDB (JH3libInterface.StoreType.H3_STORE_ROCKSDB),
    /** Redis cluster (not available). */
    H3_STORE_REDIS (JH3libInterface.StoreType.H3_STORE_REDIS),
    /** IME cluster (not available). */
    H3_STORE_IME (JH3libInterface.StoreType.H3_STORE_IME),
    /** Not an option, used for iteration purposes. */
    H3_STORE_NumOfStores (JH3libInterface.StoreType.H3_STORE_NumOfStores);

    private final int storeType;
    private static final Map<Integer,H3StoreType> lookup = new HashMap<>();

    // Map int -> H3StoreType enum for easy lookup later
    static {
        for( H3StoreType s : EnumSet.allOf(H3StoreType.class)){
            lookup.put(s.getStoreType(), s);
        }
    }

    H3StoreType(int storeType){
        this.storeType = storeType;
    }

    public int getStoreType() {
        return storeType;
    }

    /**
     *Get a H3Storetype from its integer representation.
     * @param id  Integer representation of the store type
     * @return
     */
    public static H3StoreType fromInt(int id) {
        return lookup.get(id);
    }
}