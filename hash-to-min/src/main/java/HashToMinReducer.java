import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

public class HashToMinReducer extends Reducer<Text, ClusterWritable, Text, Text> {
    public void reduce(Text key, Iterable<ClusterWritable> values, Context context) throws IOException, InterruptedException {
        Set<String> unionCluster = new HashSet<>();
        for (ClusterWritable cluster : values) {
            unionCluster.addAll(cluster.getCluster());
        }

        List<String> sorted = new ArrayList<>(unionCluster);
        Collections.sort(sorted);
        context.write(key, new Text(String.join(",", sorted)));
    }
}

