import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class AproximateWeighted {
    public static class EdgeWritable implements WritableComparable<EdgeWritable> {
        public int u, v;
        public int weight;

        public EdgeWritable() {}

        public EdgeWritable(int u, int v, int weight) {
            this.u = u;
            this.v = v;
            this.weight = weight;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(u);
            out.writeInt(v);
            out.writeInt(weight);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            u = in.readInt();
            v = in.readInt();
            weight = in.readInt();
        }

        @Override
        public int compareTo(EdgeWritable o) {
            return Integer.compare(this.weight, o.weight); // Sort by ascending weight
        }

        @Override
        public String toString() {
            return u + "\t" + v + "\t" + weight;
        }
    }

    public static class BucketMapper extends Mapper<Object, Text, IntWritable, EdgeWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] tokens = line.split("\t");

            try {
                int u = Integer.parseInt(tokens[0]);
                int v = Integer.parseInt(tokens[1]);
                int w = (tokens.length >= 3) ? Integer.parseInt(tokens[2]) : 1; // Default weight = 1

                if (w <= 0) return;

                int bucket = (int) Math.ceil(Math.log(w + 1) / Math.log(2)); // Better spread
                context.write(new IntWritable(bucket), new EdgeWritable(u, v, w));
            } catch (NumberFormatException e) {
                // Skip line
            }
        }
    }

    public static class MatchingReducer extends Reducer<IntWritable, EdgeWritable, Text, Text> {
        public void reduce(IntWritable key, Iterable<EdgeWritable> values, Context context) throws IOException, InterruptedException {
            Set<Integer> matchedVertices = new HashSet<>();
            List<EdgeWritable> edges = new ArrayList<>();

            for (EdgeWritable e : values) {
                edges.add(new EdgeWritable(e.u, e.v, e.weight));
            }

            Collections.sort(edges);

            for (EdgeWritable e : edges) {
                if (!matchedVertices.contains(e.u) && !matchedVertices.contains(e.v)) {
                    matchedVertices.add(e.u);
                    matchedVertices.add(e.v);
                    context.write(new Text(e.u + "\t" + e.v), new Text("weight=" + e.weight));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Approximate Weighted Matching");

        job.setJarByClass(AproximateWeighted.class);
        job.setMapperClass(BucketMapper.class);
        job.setReducerClass(MatchingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(EdgeWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

