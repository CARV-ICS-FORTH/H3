package gr.forth.ics.JH3lib;

/**
 * User authentication info.
 * @author Giorgos Kalaentzis
 * @version 0.1-beta
 */
public class JH3Auth {

    private int userId;

    /**
     * Create user authentication info.
     * @param userId The id of the user
     */
    public JH3Auth(int userId) {
        this.userId = userId;
    }

    /**
     * Get the ID of the user.
     * @return The ID of the user.
     */
    public int getUserId() {
        return userId;
    }

    @Override
    public String toString() {
        return "JH3Auth{" +
                "userId=" + userId +
                '}';
    }
}
