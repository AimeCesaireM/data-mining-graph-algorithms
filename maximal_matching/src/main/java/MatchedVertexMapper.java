import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper used to identify vertices that are endpoints of matched edges
public class MatchedVertexMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] vertices = line.split("\\s+|,");

        if (vertices.length == 2) {
            String u = vertices[0];
            String v = vertices[1];

            // Emit both source/target nodes
            context.write(new Text(u), new Text("matched"));
            context.write(new Text(v), new Text("matched"));
        }
    }
}