package gr.forth.ics.JH3lib;

import java.util.Arrays;

public class H3Object {
    private byte[] data;
    private long size;

    public H3Object(byte[] data, long size) {
        this.data = data;
        this.size = size;
    }

    public H3Object() {}

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public byte[] getData() {
        return data;
    }

    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "H3Object{" +
                "data=" + Arrays.toString(data) +
                ", size=" + size +
                '}';
    }
}
