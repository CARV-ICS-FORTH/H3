package gr.forth.ics.JH3lib;

// TODO Add javadoc of usage etc
public class JH3Attribute {

    private JH3AttributeType type;
    private int mode;
    private int uid;
    private int gid;

    public JH3Attribute(JH3AttributeType type, int mode) {
        this.type = type;
        this.mode = mode;
        this.uid = -1;
        this.gid = -1;
    }

    public JH3Attribute(JH3AttributeType type, int uid, int gid){
        this.type = type;
        this.mode = -1;
        this.uid = -1;
        this.gid = -1;
    }

    // TODO only show mode / uid+gid based on Attribute type;
    @Override
    public String toString() {
        return "JH3Attribute{" +
            " type='" + type + "'" +
            ", mode='" + mode + "'" +
            ", uid='" + uid + "'" +
            ", gid='" + gid + "'" +
            "}";
    }


    public JH3AttributeType getType() {
        return this.type;
    }

    public int getMode() {
        return this.mode;
    }

    public int getUid() {
        return this.uid;
    }

    public int getGid() {
        return this.gid;
    }
    
    
}