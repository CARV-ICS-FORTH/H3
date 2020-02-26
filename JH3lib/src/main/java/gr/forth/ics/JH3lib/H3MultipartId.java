package gr.forth.ics.JH3lib;

/**
 * Information about multipart identification.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class H3MultipartId {
    private String multipartId;

    public H3MultipartId(String multipartId) {
        this.multipartId = multipartId;
    }


    /**
     * Get the ID of the multipart object.
     * @return the ID of the multipart object.
     */
    public String getMultipartId() {
        return multipartId;
    }

    @Override
    public String toString() {
        return "H3MultipartId{" +
                "multipartId='" + multipartId + '\'' +
                '}';
    }
}
