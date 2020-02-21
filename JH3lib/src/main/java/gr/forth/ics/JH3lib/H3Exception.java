package gr.forth.ics.JH3lib;

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
