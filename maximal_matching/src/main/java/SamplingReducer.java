import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

// Reducer that collects proposed edges after sampling
public class SamplingReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Iterate through values (neighbors ofp proposed status form mapper).
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String status = parts[1];
            String edgeString = parts[2] + "," + parts[3];

            if ("proposed".equals(status)) {
                context.write(new Text(edgeString), NullWritable.get());
            }
        }
    }
}