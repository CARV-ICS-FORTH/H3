package gr.forth.ics.JH3lib;

/**
 * A time interval broken down in seconds and nanoseconds.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3Timespec {
    private long seconds;
    private long nanoseconds;

    /**
     * Create a Timespec object holding an interval.
     * @param seconds       The seconds of the interval
     * @param nanoseconds   The nanoseconds of the interval
     */
    public JH3Timespec(long seconds, long nanoseconds) {
        this.seconds = seconds;
        this.nanoseconds = nanoseconds;
    }

    /**
     * Create a Timespec object without nanosecond resolution.
     * @param seconds       The seconds of the interval
     */
    public JH3Timespec(long seconds) {
        this.seconds = seconds;
    }

    public long getSeconds() {
        return this.seconds;
    }

    public long getNanoseconds() {
        return this.nanoseconds;
    }

    @Override
    public String toString() {
        return "JH3Timespec{" +
            " seconds='" + seconds + "'" +
            ", nanoseconds='" + nanoseconds + "'" +
            "}";
    }

}