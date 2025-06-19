import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Reducer for identifying unique matched vertices.
public class MatchedVertexReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Each key is unique vertex (endpoint form matched edge)
        context.write(key, NullWritable.get());
    }
}