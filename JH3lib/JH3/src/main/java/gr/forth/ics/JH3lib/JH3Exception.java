package gr.forth.ics.JH3lib;

/**
 * Generic JH3 Exception. Used when the client receives an unknown status from native h3lib
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3Exception extends Exception {

    public JH3Exception(String message) {
        super(message);
    }

    public JH3Exception(String message, Throwable cause) {
        super(message, cause);
    }

    public JH3Exception(Throwable cause) {
        super(cause);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
