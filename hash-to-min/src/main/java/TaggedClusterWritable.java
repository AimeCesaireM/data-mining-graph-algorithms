import java.util.Set;

public class TaggedClusterWritable extends ClusterWritable {
    private String tag;
    private Set<String> cluster;

    public String getTag() {
        return tag;
    }
    public void setTag(String tag) {
        this.tag = tag;
    }


}