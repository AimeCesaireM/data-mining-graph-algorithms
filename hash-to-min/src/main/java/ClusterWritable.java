import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ClusterWritable implements Writable {
    private Set<String> cluster = new HashSet<>();

    public void write(DataOutput out) throws IOException {
        out.writeInt(cluster.size());
        for (String node : cluster) out.writeUTF(node);
    }

    public void readFields(DataInput in) throws IOException {
        cluster.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) cluster.add(in.readUTF());
    }

    public Set<String> getCluster() { return cluster; }

    public void setCluster(Set<String> c) { cluster = c; }

    public void merge(ClusterWritable other) {
        cluster.addAll(other.getCluster());
    }

    @Override
    public String toString() {
        return String.join(",", cluster);
    }
}
