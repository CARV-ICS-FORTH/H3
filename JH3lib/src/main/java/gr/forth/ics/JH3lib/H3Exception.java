package gr.forth.ics.JH3lib;

/**
 * Generic H3 Exception. Used when the client receives an unknown status from native h3lib
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3Exception extends Exception {

    public H3Exception(String message) {
        super(message);
    }

    public H3Exception(String message, Throwable cause) {
        super(message, cause);
    }

    public H3Exception(Throwable cause) {
        super(cause);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
