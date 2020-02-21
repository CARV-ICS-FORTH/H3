package gr.forth.ics.JH3lib;

/**
 * Brief information on individual parts of a multi-part object
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3PartInfo {
    private int partNumber;     // Part number
    private long size;          // Part size

    public H3PartInfo(int partNumber, long size) {
        this.partNumber = partNumber;
        this.size = size;
    }

    public int getPartNumber() {
        return partNumber;
    }

    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "H3PartInfo{" +
                "partNumber=" + partNumber +
                ", size=" + size +
                '}';
    }
}
