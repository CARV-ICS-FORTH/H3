package gr.forth.ics.JH3lib;

/**
 * Brief information on individual parts of a multipart object.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3PartInfo {
    private int partNumber;
    private long size;

    /**
     *
     * @param partNumber The part number of the multipart
     * @param size       The size of the part
     */
    public JH3PartInfo(int partNumber, long size) {
        this.partNumber = partNumber;
        this.size = size;
    }

    /**
     * Get the assigned part number inside of the multipart object.
     * @return the part number.
     */
    public int getPartNumber() {
        return partNumber;
    }

    /**
     * Get the size of the part object.
     * @return the size of the part object.
     */
    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "JH3PartInfo{" +
                "partNumber=" + partNumber +
                ", size=" + size +
                '}';
    }
}
