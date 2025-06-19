import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

// Mapper for sampling edges (with randomness)
public class SamplingMapper extends Mapper<LongWritable, Text, Text, Text> {

    private double samplingProbability;
    private Random random;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        long currentEdgeSetSize = conf.getLong("current_edges_count", -1);
        long mem_thresh = conf.getLong("mem_thresh", -1);

        // Calculate sampling probability based on current edge set size and threshold (Lattanzi et al., 2011).
        samplingProbability = (double) mem_thresh / (10.0 * currentEdgeSetSize);
        random = new Random();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vertices = line.split("\\s+|,");

        if (vertices.length == 2) {
            String u = vertices[0];
            String v = vertices[1];

            String edgeString;
            // To ensure same edge representation
            if (u.compareTo(v) < 0) {
                edgeString = u + "," + v;
            } else {
                edgeString = v + "," + u;
            }

            // Randomly decide to sample/propose this edge
            if (random.nextDouble() < samplingProbability) {
                // Emit both endpoints if ege was proposed.
                context.write(new Text(u), new Text(v + ",proposed," + edgeString));
                context.write(new Text(v), new Text(u + ",proposed," + edgeString));
            } else {
                context.write(new Text(u), new Text(v + ",not_proposed," + edgeString));
                context.write(new Text(v), new Text(u + ",not_proposed," + edgeString));
            }
        }
    }
}