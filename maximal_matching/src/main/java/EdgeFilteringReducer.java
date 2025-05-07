import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

// Reducer for edge filtering
public class EdgeFilteringReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private Counter edgeCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        edgeCount = context.getCounter("GraphStats", "RemainingEdges");
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
        // Only increment counter per each edge that written (and not the filtered ones)
        edgeCount.increment(1);
    }
}