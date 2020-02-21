package gr.forth.ics.JH3lib;

public class H3MultipartId {
    private String multipartId;

    public H3MultipartId(String multipartId) {
        this.multipartId = multipartId;
    }

    public String getMultipartId() {
        return multipartId;
    }

    public void setMultipartId(String multipartId) {
        this.multipartId = multipartId;
    }

    @Override
    public String toString() {
        return "H3MultipartId{" +
                "multipartId='" + multipartId + '\'' +
                '}';
    }
}
