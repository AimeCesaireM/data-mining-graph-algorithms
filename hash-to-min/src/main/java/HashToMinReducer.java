import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class HashToMinReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //to be defined
        super.setup(context);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //to be defined
        super.reduce(key, values, context);
    }
}
