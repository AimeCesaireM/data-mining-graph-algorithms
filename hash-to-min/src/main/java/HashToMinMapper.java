import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class HashToMinMapper extends Mapper<LongWritable, Text, Text, ClusterWritable> {
    private Text word = new Text();
    private ClusterWritable clusterWritable = new ClusterWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        String node = parts[0];
        String[] neighbors = parts.length > 1 ? parts[1].split(",") : new String[0];

        Set<String> cluster = new HashSet<>();
        cluster.add(node);
        Collections.addAll(cluster, neighbors);

        // Find minimum ID in cluster
        String minNode = Collections.min(cluster);

        // Emit full cluster to reducer for minNode
        word.set(minNode);
        this.clusterWritable.setCluster(new HashSet<>(cluster));
        context.write(word, this.clusterWritable);

        // Emit minNode to all others
        ClusterWritable minOnly = new ClusterWritable();
        minOnly.setCluster(Set.of(minNode));
        for (String neighbor : cluster) {
            if (!neighbor.equals(minNode)) {
                word.set(neighbor);
                context.write(word, minOnly);
            }
        }
    }
}
